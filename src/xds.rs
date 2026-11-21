// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;

use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    aggregated_discovery_service_server::AggregatedDiscoveryService,
};
use envoy_types::pb::google::protobuf::{Any, value::Kind};
use futures::Stream;
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, info_span, warn};

use crate::resources::{CDS, EDS, LDS, Snapshot};
use crate::status::{Advertisement, Advertisements, Registry};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[derive(Clone)]
pub struct Ads {
    pub snapshots: watch::Receiver<Option<Arc<Snapshot>>>,
    pub advertisements: Registry,
}

#[derive(Default)]
struct Subscription {
    names: BTreeSet<String>,
    sent_version: String,
    accepted_version: String,
    nonce: String,
}

#[derive(Default)]
struct Session {
    peer: String,
    node_id: String,
    subscriptions: BTreeMap<String, Subscription>,
    nonce: u64,
    listen_addresses: Vec<IpAddr>,
    sent_advertisements: Advertisements,
    accepted_advertisements: Advertisements,
}

impl Session {
    fn request(&mut self, request: DiscoveryRequest) -> Result<(), Status> {
        if ![CDS, EDS, LDS].contains(&request.type_url.as_str()) {
            return Err(Status::invalid_argument(
                "only CDS, EDS, and LDS are supported",
            ));
        }
        if !request.resource_locators.is_empty() {
            return Err(Status::unimplemented("resource locators are not supported"));
        }
        if let Some(value) = request
            .node
            .as_ref()
            .and_then(|node| node.metadata.as_ref())
            .and_then(|metadata| metadata.fields.get("listen_addresses"))
        {
            let invalid = || {
                Status::invalid_argument(
                    "node.metadata.listen_addresses must be a non-empty list of concrete IP addresses",
                )
            };
            let Some(Kind::ListValue(list)) = &value.kind else {
                return Err(invalid());
            };
            let mut addresses = BTreeSet::new();
            for value in &list.values {
                let Some(Kind::StringValue(value)) = &value.kind else {
                    return Err(invalid());
                };
                let address: IpAddr = value.parse().map_err(|_| invalid())?;
                if address.is_unspecified() || address.is_multicast() {
                    return Err(invalid());
                }
                addresses.insert(address);
            }
            if addresses.is_empty() {
                return Err(invalid());
            }
            let addresses: Vec<_> = addresses.into_iter().collect();
            if !self.listen_addresses.is_empty() && self.listen_addresses != addresses {
                return Err(Status::invalid_argument(
                    "listen_addresses cannot change within an ADS stream; reconnect with the new node metadata",
                ));
            }
            self.listen_addresses = addresses;
        }
        if request.type_url == LDS && self.listen_addresses.is_empty() {
            return Err(Status::invalid_argument(
                "LDS requires node.metadata.listen_addresses; send it on the first request or the LDS request",
            ));
        }
        let subscription = self
            .subscriptions
            .entry(request.type_url.clone())
            .or_default();
        if !request.response_nonce.is_empty() && request.response_nonce != subscription.nonce {
            warn!(peer = %self.peer, node_id = %self.node_id, type_url = request.type_url, nonce = request.response_nonce, expected_nonce = subscription.nonce, "ignoring stale xDS nonce");
            return Ok(());
        }
        if let Some(error) = request.error_detail {
            warn!(
                peer = %self.peer,
                node_id = %self.node_id,
                type_url = request.type_url,
                version = subscription.sent_version,
                nonce = request.response_nonce,
                code = error.code,
                error_message = error.message,
                "Envoy rejected xDS configuration"
            );
        } else if !request.response_nonce.is_empty() {
            if request.version_info == subscription.sent_version {
                let newly_accepted = subscription.accepted_version != request.version_info;
                subscription.accepted_version = request.version_info.clone();
                if newly_accepted {
                    info!(peer = %self.peer, node_id = %self.node_id, type_url = request.type_url, version = request.version_info, nonce = request.response_nonce, "Envoy accepted xDS configuration");
                }
                if request.type_url == LDS {
                    self.accepted_advertisements = self.sent_advertisements.clone();
                }
            }
            debug!(
                type_url = request.type_url,
                version = request.version_info,
                "Envoy acknowledged xDS configuration"
            );
        }
        let names = request.resource_names.into_iter().collect();
        if subscription.nonce.is_empty() || subscription.names != names {
            info!(peer = %self.peer, node_id = %self.node_id, type_url = request.type_url, resource_names = ?names, "ADS subscription updated");
        }
        if subscription.names != names {
            subscription.names = names;
            subscription.sent_version.clear();
            subscription.accepted_version.clear();
        }
        Ok(())
    }

    fn dependencies_accepted(&self, snapshot: &Snapshot) -> bool {
        // Removing all listeners needs no upstream resources. Otherwise wait for
        // CDS and every EDS assignment to be accepted before exposing listeners.
        if snapshot.clusters.is_empty() {
            return true;
        }
        [(CDS, &snapshot.clusters), (EDS, &snapshot.endpoints)]
            .into_iter()
            .all(|(type_url, resources)| {
                self.subscriptions
                    .get(type_url)
                    .is_some_and(|subscription| {
                        subscription.accepted_version == version(type_url, resources)
                            && resources.keys().all(|name| {
                                subscription.names.contains(name)
                                    || (type_url == CDS && subscription.names.is_empty())
                            })
                    })
            })
    }

    fn responses(&mut self, snapshot: &Snapshot) -> Vec<DiscoveryResponse> {
        let mut responses = Vec::new();
        // Deliver dependencies first when the client has subscribed to them.
        for (type_url, resources) in [
            (CDS, &snapshot.clusters),
            (EDS, &snapshot.endpoints),
            (LDS, &snapshot.listeners),
        ] {
            if type_url == LDS && !self.dependencies_accepted(snapshot) {
                continue;
            }
            let Some(subscription) = self.subscriptions.get_mut(type_url) else {
                continue;
            };
            let mut version = version(type_url, resources);
            if type_url == LDS {
                // Recreated Services must be acknowledged again even if their ports are unchanged.
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                snapshot.services.hash(&mut hasher);
                version.push_str(&format!("-{:016x}", hasher.finish()));
            }
            if subscription.sent_version == version {
                continue;
            }
            let resources: Vec<Any> = resources
                .iter()
                .filter(|(name, _)| {
                    subscription.names.contains(*name)
                        || (subscription.names.is_empty() && type_url != EDS)
                })
                .map(|(_, resource)| resource.clone())
                .collect();
            if type_url == LDS {
                self.sent_advertisements = snapshot
                    .services
                    .iter()
                    .filter_map(|(service, listeners)| {
                        let addresses: BTreeSet<_> = self
                            .listen_addresses
                            .iter()
                            .copied()
                            .filter(|ip| {
                                !listeners.is_empty()
                                    && listeners.iter().all(|name| {
                                        let name = format!("{name}/{ip}");
                                        snapshot.listeners.contains_key(&name)
                                            && (subscription.names.is_empty()
                                                || subscription.names.contains(&name))
                                    })
                            })
                            .collect();
                        if addresses.is_empty() {
                            return None;
                        }
                        Some((
                            service.clone(),
                            Advertisement {
                                addresses,
                                listeners: listeners.clone(),
                            },
                        ))
                    })
                    .collect();
            }
            self.nonce += 1;
            subscription.nonce = self.nonce.to_string();
            subscription.sent_version = version.clone();
            let span = info_span!("xds.response", peer = %self.peer, node_id = %self.node_id, %type_url, %version, nonce = subscription.nonce, resources = resources.len() as i64);
            crate::telemetry::attributes(
                &span,
                "resource_names",
                &serde_json::json!(subscription.names),
            );
            span.in_scope(|| info!(peer = %self.peer, node_id = %self.node_id, %type_url, %version, nonce = subscription.nonce, resources = resources.len() as i64, resource_names = ?subscription.names, "sending xDS response"));
            responses.push(DiscoveryResponse {
                version_info: version,
                resources,
                type_url: type_url.into(),
                nonce: subscription.nonce.clone(),
                ..Default::default()
            });
        }
        responses
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let versions: BTreeMap<_, _> = self.subscriptions.iter().map(|(kind, subscription)| {
            (kind, serde_json::json!({"sent": subscription.sent_version, "accepted": subscription.accepted_version}))
        }).collect();
        let span = info_span!("xds.stream_closed", peer = %self.peer, node_id = %self.node_id);
        crate::telemetry::attributes(&span, "versions", &serde_json::json!(versions));
        span.in_scope(|| info!(peer = %self.peer, node_id = %self.node_id, versions = %serde_json::json!(versions), "ADS stream closed"));
    }
}

fn version(type_url: &str, resources: &BTreeMap<String, Any>) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    type_url.hash(&mut hasher);
    for (name, resource) in resources {
        name.hash(&mut hasher);
        resource.value.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

#[tonic::async_trait]
impl AggregatedDiscoveryService for Ads {
    type StreamAggregatedResourcesStream = ResponseStream<DiscoveryResponse>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let peer = request
            .remote_addr()
            .map(|address| address.to_string())
            .unwrap_or_else(|| "unknown".into());
        let mut requests = request.into_inner();
        let mut snapshots = self.snapshots.clone();
        let registration = self.advertisements.register();
        let output = async_stream::try_stream! {
            let mut session = Session {
                peer: peer.clone(), node_id: String::new(), subscriptions: BTreeMap::new(),
                nonce: 0, listen_addresses: Vec::new(),
                sent_advertisements: Advertisements::new(), accepted_advertisements: Advertisements::new(),
            };
            info_span!("xds.stream_opened", %peer).in_scope(|| info!(%peer, "ADS stream opened"));
            let mut identified = false;
            loop {
                let request = tokio::select! {
                    request = requests.message() => Some(request),
                    changed = snapshots.changed() => {
                        if changed.is_err() { break; }
                        None
                    }
                };
                if let Some(request) = request {
                    let Some(request) = request.map_err(|error| {
                        info_span!("xds.receive_error", %peer).in_scope(|| warn!(%peer, %error, "ADS receive failed"));
                        error
                    })? else { break };
                    let span = info_span!("xds.request", %peer,
                        node_id = request.node.as_ref().map(|node| node.id.as_str()).unwrap_or(&session.node_id),
                        type_url = request.type_url, version = request.version_info, nonce = request.response_nonce);
                    crate::telemetry::attributes(&span, "resource_names", &serde_json::json!(request.resource_names));
                    let _entered = span.enter();
                    let new_subscription = !session.subscriptions.contains_key(&request.type_url);
                    let type_url = request.type_url.clone();
                    let node = request.node.as_ref().map(|node| (node.id.clone(), node.cluster.clone()));
                    if let Some((node_id, _)) = &node {
                        session.node_id = node_id.clone();
                    }
                    session.request(request).map_err(|error| {
                        warn!(%peer, %error, "invalid ADS request; closing stream");
                        error
                    })?;
                    if !identified && let Some((node_id, node_cluster)) = node {
                        info!(%peer, %node_id, %node_cluster, listen_addresses = ?session.listen_addresses, "ADS client identified");
                        crate::telemetry::attributes(&span, "listen_addresses", &serde_json::json!(session.listen_addresses));
                        span.record("node_id", &node_id);
                        identified = true;
                    }
                    if new_subscription {
                        info!(%peer, %type_url, "ADS subscription opened");
                    }
                    registration.publish(&session.accepted_advertisements);
                }
                // Never send an empty startup snapshot before both Kubernetes watches synchronize.
                let snapshot = snapshots.borrow_and_update().clone();
                if let Some(snapshot) = snapshot {
                    let snapshot = snapshot.for_addresses(&session.listen_addresses)
                        .map_err(|error| Status::internal(format!("cannot bind listeners: {error}")))?;
                    for response in session.responses(&snapshot) {
                        yield response;
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
    }

    type DeltaAggregatedResourcesStream = ResponseStream<DeltaDiscoveryResponse>;

    async fn delta_aggregated_resources(
        &self,
        _: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaAggregatedResourcesStream>, Status> {
        Err(Status::unimplemented(
            "use state-of-the-world ADS (api_type: GRPC)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{
        build,
        tests::{service, slice},
    };
    use envoy_types::pb::{
        envoy::service::discovery::v3::{
            aggregated_discovery_service_client::AggregatedDiscoveryServiceClient,
            aggregated_discovery_service_server::AggregatedDiscoveryServiceServer,
        },
        google::rpc,
    };
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::{Duration, timeout};
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

    fn snapshot() -> Snapshot {
        build([&service()], &[slice()]).unwrap()
    }

    fn request(type_url: &str) -> DiscoveryRequest {
        DiscoveryRequest {
            type_url: type_url.into(),
            node: Some(node(&["203.0.113.10", "2001:db8::10"])),
            ..Default::default()
        }
    }

    fn pending_listener_ack(snapshot: &Snapshot) -> (Session, DiscoveryResponse) {
        let mut session = Session::default();
        session.request(request(LDS)).unwrap();
        session.request(request(CDS)).unwrap();
        let cds = session.responses(snapshot).remove(0);
        session
            .request(DiscoveryRequest {
                response_nonce: cds.nonce,
                version_info: cds.version_info,
                ..request(CDS)
            })
            .unwrap();
        let names: Vec<_> = snapshot.endpoints.keys().cloned().collect();
        session
            .request(DiscoveryRequest {
                resource_names: names.clone(),
                ..request(EDS)
            })
            .unwrap();
        let eds = session.responses(snapshot).remove(0);
        session
            .request(DiscoveryRequest {
                response_nonce: eds.nonce,
                version_info: eds.version_info,
                resource_names: names,
                ..request(EDS)
            })
            .unwrap();
        let lds = session.responses(snapshot).remove(0);
        (session, lds)
    }

    #[test]
    fn status_requires_lds_ack_and_keeps_last_accepted_state_after_nack() {
        let snapshot = snapshot()
            .for_addresses(&[
                "203.0.113.10".parse().unwrap(),
                "2001:db8::10".parse().unwrap(),
            ])
            .unwrap();
        let (mut session, lds) = pending_listener_ack(&snapshot);
        assert!(session.accepted_advertisements.is_empty());
        let ack = DiscoveryRequest {
            response_nonce: lds.nonce,
            version_info: lds.version_info,
            ..request(LDS)
        };
        session
            .request(DiscoveryRequest {
                response_nonce: "stale".into(),
                ..ack.clone()
            })
            .unwrap();
        assert!(session.accepted_advertisements.is_empty());
        session
            .request(DiscoveryRequest {
                error_detail: Some(rpc::Status::default()),
                ..ack.clone()
            })
            .unwrap();
        assert!(session.accepted_advertisements.is_empty());
        session.request(ack).unwrap();
        let accepted = session.accepted_advertisements.clone();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted.values().next().unwrap().addresses.len(), 2);

        let mut recreated = snapshot.clone();
        recreated.services = recreated
            .services
            .into_iter()
            .map(|(mut identity, names)| {
                identity.uid = "recreated-service".into();
                (identity, names)
            })
            .collect();
        let lds = session.responses(&recreated).remove(0);
        session
            .request(DiscoveryRequest {
                response_nonce: lds.nonce.clone(),
                version_info: String::new(),
                error_detail: Some(rpc::Status::default()),
                ..request(LDS)
            })
            .unwrap();
        assert_eq!(session.accepted_advertisements, accepted);
        session
            .request(DiscoveryRequest {
                response_nonce: lds.nonce,
                version_info: lds.version_info,
                ..request(LDS)
            })
            .unwrap();
        assert!(
            session
                .accepted_advertisements
                .keys()
                .all(|identity| identity.uid == "recreated-service")
        );
    }

    #[test]
    fn partial_listener_subscriptions_publish_only_fully_covered_addresses() {
        let snapshot = snapshot()
            .for_addresses(&[
                "203.0.113.10".parse().unwrap(),
                "2001:db8::10".parse().unwrap(),
            ])
            .unwrap();
        let (mut session, _) = pending_listener_ack(&snapshot);
        let ipv4: Vec<_> = snapshot
            .listeners
            .keys()
            .filter(|name| name.ends_with("/203.0.113.10"))
            .cloned()
            .collect();
        session
            .request(DiscoveryRequest {
                resource_names: ipv4.clone(),
                ..request(LDS)
            })
            .unwrap();
        let lds = session.responses(&snapshot).remove(0);
        session
            .request(DiscoveryRequest {
                response_nonce: lds.nonce,
                version_info: lds.version_info,
                resource_names: ipv4.clone(),
                ..request(LDS)
            })
            .unwrap();
        assert_eq!(
            session
                .accepted_advertisements
                .values()
                .next()
                .unwrap()
                .addresses,
            ["203.0.113.10".parse().unwrap()].into()
        );
        let partial = vec![ipv4[0].clone()];
        session
            .request(DiscoveryRequest {
                resource_names: partial.clone(),
                ..request(LDS)
            })
            .unwrap();
        let lds = session.responses(&snapshot).remove(0);
        session
            .request(DiscoveryRequest {
                response_nonce: lds.nonce,
                version_info: lds.version_info,
                resource_names: partial,
                ..request(LDS)
            })
            .unwrap();
        assert!(session.accepted_advertisements.is_empty());
    }

    fn node(addresses: &[&str]) -> envoy_types::pb::envoy::config::core::v3::Node {
        use envoy_types::pb::google::protobuf::{ListValue, Struct, Value};
        envoy_types::pb::envoy::config::core::v3::Node {
            metadata: Some(Struct {
                fields: [(
                    "listen_addresses".into(),
                    Value {
                        kind: Some(Kind::ListValue(ListValue {
                            values: addresses
                                .iter()
                                .map(|address| Value {
                                    kind: Some(Kind::StringValue((*address).into())),
                                })
                                .collect(),
                        })),
                    },
                )]
                .into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn listener_metadata_is_required_validated_and_retained_per_stream() {
        use envoy_types::pb::google::protobuf::{Struct, Value};
        for kind in [
            None,
            Some(Kind::StringValue("203.0.113.10".into())),
            Some(Kind::NumberValue(10.0)),
        ] {
            let mut invalid_node = node(&["203.0.113.10"]);
            invalid_node.metadata = Some(Struct {
                fields: [("listen_addresses".into(), Value { kind })].into(),
            });
            assert!(
                Session::default()
                    .request(DiscoveryRequest {
                        node: Some(invalid_node),
                        ..request(LDS)
                    })
                    .is_err()
            );
        }
        for addresses in [
            vec![],
            vec!["bad"],
            vec!["0.0.0.0"],
            vec!["::"],
            vec!["224.0.0.1"],
            vec!["ff02::1"],
        ] {
            let mut session = Session::default();
            let error = session
                .request(DiscoveryRequest {
                    node: Some(node(&addresses)),
                    ..request(LDS)
                })
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
        let mut session = Session::default();
        session
            .request(DiscoveryRequest {
                node: None,
                ..request(CDS)
            })
            .unwrap();
        session
            .request(DiscoveryRequest {
                node: None,
                ..request(EDS)
            })
            .unwrap();
        assert!(
            session
                .request(DiscoveryRequest {
                    node: None,
                    ..request(LDS)
                })
                .is_err()
        );
        session.request(request(CDS)).unwrap();
        session
            .request(DiscoveryRequest {
                node: None,
                ..request(LDS)
            })
            .unwrap();
        assert_eq!(session.listen_addresses.len(), 2);
        assert!(
            session
                .request(DiscoveryRequest {
                    node: Some(node(&["203.0.113.11"])),
                    ..request(LDS)
                })
                .is_err()
        );
        let mut other = Session::default();
        other
            .request(DiscoveryRequest {
                node: Some(node(&["203.0.113.11", "203.0.113.11"])),
                ..request(LDS)
            })
            .unwrap();
        assert_eq!(
            other.listen_addresses,
            ["203.0.113.11".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn listener_addresses_are_isolated_and_new_ports_use_the_same_addresses() {
        use envoy_types::pb::envoy::config::{
            core::v3::{
                address::Address,
                socket_address::{PortSpecifier, Protocol},
            },
            listener::v3::Listener,
        };
        use prost::Message;
        let snapshot = snapshot();
        let addresses = [
            "203.0.113.10".parse().unwrap(),
            "2001:db8::10".parse().unwrap(),
        ];
        let bound = snapshot.for_addresses(&addresses).unwrap();
        assert_eq!(bound.clusters, snapshot.clusters);
        assert_eq!(bound.endpoints, snapshot.endpoints);
        assert_eq!(bound.listeners.len(), 4);
        for (name, resource) in &bound.listeners {
            let listener = Listener::decode(resource.value.as_slice()).unwrap();
            assert_eq!(&listener.name, name);
            let Some(Address::SocketAddress(socket)) = listener.address.unwrap().address else {
                panic!("socket missing")
            };
            assert!(addresses.contains(&socket.address.parse::<IpAddr>().unwrap()));
            assert_eq!(socket.port_specifier, Some(PortSpecifier::PortValue(443)));
            assert_eq!(
                socket.protocol,
                if name.contains("/udp/") {
                    Protocol::Udp as i32
                } else {
                    Protocol::Tcp as i32
                }
            );
            assert!(!socket.ipv4_compat);
        }
        let other = snapshot
            .for_addresses(&["203.0.113.11".parse().unwrap()])
            .unwrap();
        assert_ne!(
            version(LDS, &bound.listeners),
            version(LDS, &other.listeners)
        );
        assert!(
            other
                .listeners
                .keys()
                .all(|name| name.ends_with("/203.0.113.11"))
        );
        assert!(snapshot.for_addresses(&[]).unwrap().listeners.is_empty());

        let mut service = service();
        let ports = service.spec.as_mut().unwrap().ports.as_mut().unwrap();
        let mut added = ports[0].clone();
        added.port = 8443;
        ports.push(added);
        let updated = build([&service], &[slice()])
            .unwrap()
            .for_addresses(&addresses)
            .unwrap();
        assert_eq!(updated.listeners.len(), 6);
        for ip in addresses {
            assert!(
                updated
                    .listeners
                    .contains_key(&format!("ingress/envoy/tcp/8443/{ip}"))
            );
        }
    }

    #[test]
    fn ack_and_nack_do_not_cause_response_loops_and_stale_nonce_is_ignored() {
        let snapshot = snapshot();
        let mut session = Session::default();
        session.request(request(CDS)).unwrap();
        let response = session.responses(&snapshot).remove(0);
        let ack = DiscoveryRequest {
            response_nonce: response.nonce.clone(),
            version_info: response.version_info.clone(),
            ..request(CDS)
        };
        session.request(ack.clone()).unwrap();
        assert!(session.responses(&snapshot).is_empty());
        session
            .request(DiscoveryRequest {
                error_detail: Some(rpc::Status {
                    code: 3,
                    message: "bad config".into(),
                    ..Default::default()
                }),
                ..ack.clone()
            })
            .unwrap();
        assert!(session.responses(&snapshot).is_empty());
        session
            .request(DiscoveryRequest {
                response_nonce: "stale".into(),
                resource_names: vec!["other".into()],
                ..ack
            })
            .unwrap();
        assert!(session.responses(&snapshot).is_empty());
        assert!(session.subscriptions[CDS].names.is_empty());
        assert_eq!(
            session.responses(&Snapshot::default())[0].resources.len(),
            0
        );
    }

    #[test]
    fn explicit_subscriptions_filter_resources_and_changes_are_sent() {
        let snapshot = snapshot();
        let mut session = Session::default();
        let name = "ingress/envoy/tcp/443";
        session
            .request(DiscoveryRequest {
                resource_names: vec![name.into()],
                ..request(EDS)
            })
            .unwrap();
        let response = session.responses(&snapshot).remove(0);
        assert_eq!(response.resources, vec![snapshot.endpoints[name].clone()]);
        session
            .request(DiscoveryRequest {
                response_nonce: response.nonce,
                resource_names: vec!["missing".into()],
                ..request(EDS)
            })
            .unwrap();
        assert!(session.responses(&snapshot)[0].resources.is_empty());
    }

    #[test]
    fn listeners_wait_for_accepted_cluster_and_endpoint_dependencies() {
        let snapshot = snapshot();
        let mut session = Session::default();
        session.request(request(LDS)).unwrap();
        session.request(request(CDS)).unwrap();
        let responses = session.responses(&snapshot);
        assert_eq!(responses.len(), 1);
        let cds = &responses[0];
        assert_eq!(cds.type_url, CDS);
        session
            .request(DiscoveryRequest {
                response_nonce: cds.nonce.clone(),
                version_info: cds.version_info.clone(),
                ..request(CDS)
            })
            .unwrap();
        assert!(session.responses(&snapshot).is_empty());
        session
            .request(DiscoveryRequest {
                resource_names: snapshot.endpoints.keys().cloned().collect(),
                ..request(EDS)
            })
            .unwrap();
        let responses = session.responses(&snapshot);
        assert_eq!(responses.len(), 1);
        let eds = &responses[0];
        assert_eq!(eds.type_url, EDS);
        session
            .request(DiscoveryRequest {
                response_nonce: eds.nonce.clone(),
                version_info: eds.version_info.clone(),
                resource_names: snapshot.endpoints.keys().cloned().collect(),
                ..request(EDS)
            })
            .unwrap();
        let responses = session.responses(&snapshot);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].type_url, LDS);
        assert_eq!(responses[0].resources.len(), 2);
    }

    #[tokio::test]
    async fn grpc_stream_waits_for_sync_delivers_updates_and_handles_reconnects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = watch::channel(None);
        let (stop, stopped) = oneshot::channel();
        let advertisements = Registry::default();
        let mut advertised = advertisements.subscribe();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(AggregatedDiscoveryServiceServer::new(Ads {
                    snapshots: receiver,
                    advertisements,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                }),
        );
        let mut client = AggregatedDiscoveryServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let (requests, incoming) = mpsc::channel(8);
        let mut responses = client
            .stream_aggregated_resources(ReceiverStream::new(incoming))
            .await
            .unwrap()
            .into_inner();
        requests.send(request(CDS)).await.unwrap();
        assert!(
            timeout(Duration::from_millis(50), responses.message())
                .await
                .is_err()
        );
        sender.send(Some(Arc::new(snapshot()))).unwrap();
        let response = timeout(Duration::from_secs(2), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.type_url, CDS);
        assert_eq!(response.resources.len(), 2);
        requests
            .send(DiscoveryRequest {
                response_nonce: response.nonce,
                version_info: response.version_info,
                ..request(CDS)
            })
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(50), responses.message())
                .await
                .is_err()
        );
        let names: Vec<_> = snapshot().endpoints.keys().cloned().collect();
        requests
            .send(DiscoveryRequest {
                node: None,
                resource_names: names.clone(),
                ..request(EDS)
            })
            .await
            .unwrap();
        let eds = timeout(Duration::from_secs(2), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        requests
            .send(DiscoveryRequest {
                node: None,
                resource_names: names,
                response_nonce: eds.nonce,
                version_info: eds.version_info,
                ..request(EDS)
            })
            .await
            .unwrap();
        requests
            .send(DiscoveryRequest {
                node: None,
                ..request(LDS)
            })
            .await
            .unwrap();
        let lds = timeout(Duration::from_secs(2), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(lds.type_url, LDS);
        assert_eq!(lds.resources.len(), 4);
        assert!(advertised.borrow().is_empty());
        requests
            .send(DiscoveryRequest {
                node: None,
                response_nonce: lds.nonce.clone(),
                version_info: lds.version_info.clone(),
                ..request(LDS)
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(2), advertised.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            advertised
                .borrow()
                .values()
                .next()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .addresses
                .len(),
            2
        );
        for resource in lds.resources {
            use envoy_types::pb::envoy::config::{
                core::v3::address::Address, listener::v3::Listener,
            };
            use prost::Message;
            let listener = Listener::decode(resource.value.as_slice()).unwrap();
            let Some(Address::SocketAddress(socket)) = listener.address.unwrap().address else {
                panic!("socket missing")
            };
            assert!(["203.0.113.10", "2001:db8::10"].contains(&socket.address.as_str()));
            assert!(!socket.ipv4_compat);
        }
        // A pod replacement changes EDS without changing CDS/LDS or resource counts.
        let mut replacement = slice();
        replacement.endpoints.as_mut().unwrap()[0].addresses = vec!["10.244.5.66".into()];
        let latest = build([&service()], &[replacement]).unwrap();
        sender.send(Some(Arc::new(latest.clone()))).unwrap();
        let updated = timeout(Duration::from_secs(2), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(updated.type_url, EDS);
        assert_eq!(updated.version_info, version(EDS, &latest.endpoints));
        assert_eq!(
            updated.resources,
            latest.endpoints.values().cloned().collect::<Vec<_>>()
        );
        let changes = crate::snapshot_changes::between(Some(&snapshot()), &latest);
        for name in latest.endpoints.keys() {
            assert_eq!(
                changes["backends"][name]["added"],
                serde_json::json!(["10.244.5.66:10443"])
            );
            assert_eq!(
                changes["backends"][name]["removed"],
                serde_json::json!(["10.0.0.1:10443"])
            );
        }
        requests
            .send(DiscoveryRequest {
                node: None,
                resource_names: latest.endpoints.keys().cloned().collect(),
                response_nonce: updated.nonce,
                version_info: updated.version_info.clone(),
                ..request(EDS)
            })
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(50), responses.message())
                .await
                .is_err()
        );

        // Reconnecting must deliver the latest endpoints even if Envoy remembers a version.
        drop(responses);
        drop(requests);
        timeout(
            Duration::from_secs(2),
            advertised.wait_for(BTreeMap::is_empty),
        )
        .await
        .unwrap()
        .unwrap();
        let (requests, incoming) = mpsc::channel(8);
        let mut responses = client
            .stream_aggregated_resources(ReceiverStream::new(incoming))
            .await
            .unwrap()
            .into_inner();
        requests
            .send(DiscoveryRequest {
                resource_names: latest.endpoints.keys().cloned().collect(),
                version_info: updated.version_info,
                ..request(EDS)
            })
            .await
            .unwrap();
        let reconnected = timeout(Duration::from_secs(2), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reconnected.type_url, EDS);
        assert_eq!(
            reconnected.resources,
            latest.endpoints.values().cloned().collect::<Vec<_>>()
        );
        sender.send(Some(Arc::new(Snapshot::default()))).unwrap();
        assert!(
            timeout(Duration::from_secs(2), responses.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .resources
                .is_empty()
        );
        drop(responses);
        drop(requests);
        timeout(
            Duration::from_secs(2),
            advertised.wait_for(BTreeMap::is_empty),
        )
        .await
        .unwrap()
        .unwrap();
        let (requests, incoming) = mpsc::channel(8);
        let mut responses = client
            .stream_aggregated_resources(ReceiverStream::new(incoming))
            .await
            .unwrap()
            .into_inner();
        requests.send(request(LDS)).await.unwrap();
        assert!(
            timeout(Duration::from_secs(2), responses.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .resources
                .is_empty()
        );
        drop(responses);
        drop(requests);
        stop.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
