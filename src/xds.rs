// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;

use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    aggregated_discovery_service_server::AggregatedDiscoveryService,
};
use envoy_types::pb::google::protobuf::Any;
use futures::Stream;
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::resources::{CDS, EDS, LDS, Snapshot};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[derive(Clone)]
pub struct Ads {
    pub snapshots: watch::Receiver<Option<Arc<Snapshot>>>,
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
    subscriptions: BTreeMap<String, Subscription>,
    nonce: u64,
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
        let subscription = self
            .subscriptions
            .entry(request.type_url.clone())
            .or_default();
        if !request.response_nonce.is_empty() && request.response_nonce != subscription.nonce {
            debug!(type_url = request.type_url, "ignoring stale xDS nonce");
            return Ok(());
        }
        if let Some(error) = request.error_detail {
            warn!(
                type_url = request.type_url,
                nonce = request.response_nonce,
                message = error.message,
                "Envoy rejected xDS configuration"
            );
        } else if !request.response_nonce.is_empty() {
            if request.version_info == subscription.sent_version {
                subscription.accepted_version = request.version_info.clone();
            }
            debug!(
                type_url = request.type_url,
                version = request.version_info,
                "Envoy acknowledged xDS configuration"
            );
        }
        let names = request.resource_names.into_iter().collect();
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
            let version = version(type_url, resources);
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
            self.nonce += 1;
            subscription.nonce = self.nonce.to_string();
            subscription.sent_version = version.clone();
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
        let mut requests = request.into_inner();
        let mut snapshots = self.snapshots.clone();
        let output = async_stream::try_stream! {
            let mut session = Session::default();
            loop {
                let request = tokio::select! {
                    request = requests.message() => Some(request),
                    changed = snapshots.changed() => {
                        if changed.is_err() { break; }
                        None
                    }
                };
                if let Some(request) = request {
                    let Some(request) = request? else { break };
                    session.request(request)?;
                }
                // Never send an empty startup snapshot before both Kubernetes watches synchronize.
                let snapshot = snapshots.borrow_and_update().clone();
                if let Some(snapshot) = snapshot {
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
            ..Default::default()
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
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(AggregatedDiscoveryServiceServer::new(Ads {
                    snapshots: receiver,
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
