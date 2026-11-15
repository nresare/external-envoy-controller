// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use anyhow::{Context, bail};
use envoy_types::pb::envoy::config::{
    cluster::v3 as cluster, core::v3 as core, endpoint::v3 as endpoint, listener::v3 as listener,
};
use envoy_types::pb::envoy::extensions::filters::{
    network::tcp_proxy::v3::{TcpProxy, tcp_proxy},
    udp::udp_proxy::v3::{Route, UdpProxyConfig, udp_proxy_config},
};
use envoy_types::pb::google::protobuf::{Any, Duration};
use envoy_types::pb::xds::{
    core::v3::TypedExtensionConfig,
    r#type::matcher::v3::{Matcher, matcher},
};
use k8s_openapi::api::{
    core::v1::{Service, ServicePort},
    discovery::v1::EndpointSlice,
};
use kube::ResourceExt;
use prost::{Message, Name};

pub const CDS: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
pub const EDS: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
pub const LDS: &str = "type.googleapis.com/envoy.config.listener.v3.Listener";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Snapshot {
    pub clusters: BTreeMap<String, Any>,
    pub endpoints: BTreeMap<String, Any>,
    pub listeners: BTreeMap<String, Any>,
}

impl Snapshot {
    /// Instantiate listener templates for one Envoy's explicitly advertised IPs.
    pub fn for_addresses(&self, addresses: &[IpAddr]) -> anyhow::Result<Self> {
        let mut snapshot = self.clone();
        snapshot.listeners.clear();
        for resource in self.listeners.values() {
            let template = listener::Listener::decode(resource.value.as_slice())?;
            for ip in addresses {
                let mut listener = template.clone();
                listener.name = format!("{}/{}", template.name, ip);
                let socket = match listener.address.as_mut().and_then(|a| a.address.as_mut()) {
                    Some(core::address::Address::SocketAddress(socket)) => socket,
                    _ => bail!("listener template has no socket address"),
                };
                socket.address = ip.to_string();
                socket.ipv4_compat = false;
                snapshot
                    .listeners
                    .insert(listener.name.clone(), pack(&listener));
            }
        }
        Ok(snapshot)
    }
}

pub fn pack<T: Message + Name>(message: &T) -> Any {
    Any {
        type_url: T::type_url(),
        value: message.encode_to_vec(),
    }
}

fn address(
    ip: IpAddr,
    port: u16,
    protocol: core::socket_address::Protocol,
    dual_stack: bool,
) -> core::Address {
    core::Address {
        address: Some(core::address::Address::SocketAddress(core::SocketAddress {
            address: ip.to_string(),
            port_specifier: Some(core::socket_address::PortSpecifier::PortValue(u32::from(
                port,
            ))),
            protocol: protocol as i32,
            ipv4_compat: dual_stack && ip == IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
            ..Default::default()
        })),
    }
}

fn port_number(port: i32) -> anyhow::Result<u16> {
    let value = u16::try_from(port).context("port must be between 1 and 65535")?;
    anyhow::ensure!(value != 0, "port must be between 1 and 65535");
    Ok(value)
}

fn backend_addresses(
    service: &Service,
    port: &ServicePort,
    slices: &[EndpointSlice],
) -> anyhow::Result<BTreeSet<(IpAddr, u16)>> {
    let mut addresses = BTreeSet::new();
    for slice in slices {
        if slice.namespace() != service.namespace()
            || slice.labels().get("kubernetes.io/service-name") != Some(&service.name_any())
            || !matches!(slice.address_type.as_str(), "IPv4" | "IPv6")
        {
            continue;
        }
        for slice_port in slice.ports.iter().flatten() {
            if slice_port.name.as_deref().unwrap_or_default()
                != port.name.as_deref().unwrap_or_default()
                || slice_port.protocol.as_deref().unwrap_or("TCP")
                    != port.protocol.as_deref().unwrap_or("TCP")
            {
                continue;
            }
            let Some(number) = slice_port.port else {
                continue;
            };
            let number = port_number(number)?;
            for endpoint in slice.endpoints.iter().flatten() {
                if endpoint.conditions.as_ref().is_some_and(|conditions| {
                    conditions.ready == Some(false) || conditions.terminating == Some(true)
                }) {
                    continue;
                }
                for ip in &endpoint.addresses {
                    let ip: IpAddr = ip.parse().context("invalid EndpointSlice IP address")?;
                    anyhow::ensure!(
                        ip.is_ipv4() == (slice.address_type == "IPv4"),
                        "EndpointSlice address family mismatch"
                    );
                    addresses.insert((ip, number));
                }
            }
        }
    }
    Ok(addresses)
}

pub fn build<'a>(
    services: impl IntoIterator<Item = &'a Service>,
    slices: &[EndpointSlice],
) -> anyhow::Result<Snapshot> {
    let bind = IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED);
    let mut snapshot = Snapshot::default();
    let mut bindings = BTreeMap::new();
    for service in services {
        let Some(spec) = &service.spec else { continue };
        anyhow::ensure!(
            spec.load_balancer_source_ranges
                .as_ref()
                .is_none_or(Vec::is_empty),
            "loadBalancerSourceRanges are not supported on {}/{}",
            service.namespace().unwrap_or_default(),
            service.name_any()
        );
        for port in spec.ports.iter().flatten() {
            let protocol_name = port.protocol.as_deref().unwrap_or("TCP");
            let protocol = match protocol_name {
                "TCP" => core::socket_address::Protocol::Tcp,
                "UDP" => core::socket_address::Protocol::Udp,
                other => bail!(
                    "unsupported Service protocol {other} on {}/{}",
                    service.namespace().unwrap_or_default(),
                    service.name_any()
                ),
            };
            let number = port_number(port.port)?;
            let name = format!(
                "{}/{}/{}/{}",
                service.namespace().unwrap_or_default(),
                service.name_any(),
                protocol_name.to_lowercase(),
                number
            );
            if let Some(previous) = bindings.insert((protocol_name, number), name.clone()) {
                bail!(
                    "listener conflict on {bind}:{number}/{protocol_name} between {previous} and {name}"
                );
            }
            let cluster = cluster::Cluster {
                name: name.clone(),
                cluster_discovery_type: Some(cluster::cluster::ClusterDiscoveryType::Type(
                    cluster::cluster::DiscoveryType::Eds as i32,
                )),
                connect_timeout: Some(Duration {
                    seconds: 5,
                    nanos: 0,
                }),
                eds_cluster_config: Some(cluster::cluster::EdsClusterConfig {
                    eds_config: Some(core::ConfigSource {
                        resource_api_version: core::ApiVersion::V3 as i32,
                        config_source_specifier: Some(
                            core::config_source::ConfigSourceSpecifier::Ads(
                                core::AggregatedConfigSource {},
                            ),
                        ),
                        ..Default::default()
                    }),
                    service_name: name.clone(),
                }),
                ..Default::default()
            };
            let backends = backend_addresses(service, port, slices)?;
            let assignment = endpoint::ClusterLoadAssignment {
                cluster_name: name.clone(),
                endpoints: vec![endpoint::LocalityLbEndpoints {
                    lb_endpoints: backends
                        .into_iter()
                        .map(|(ip, port)| endpoint::LbEndpoint {
                            host_identifier: Some(endpoint::lb_endpoint::HostIdentifier::Endpoint(
                                endpoint::Endpoint {
                                    address: Some(address(ip, port, protocol, false)),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut listener = listener::Listener {
                name: name.clone(),
                address: Some(address(bind, number, protocol, true)),
                ..Default::default()
            };
            match protocol {
                core::socket_address::Protocol::Tcp => {
                    let proxy = TcpProxy {
                        stat_prefix: name.clone(),
                        cluster_specifier: Some(tcp_proxy::ClusterSpecifier::Cluster(name.clone())),
                        ..Default::default()
                    };
                    listener.filter_chains.push(listener::FilterChain {
                        filters: vec![listener::Filter {
                            name: "envoy.filters.network.tcp_proxy".into(),
                            config_type: Some(listener::filter::ConfigType::TypedConfig(pack(
                                &proxy,
                            ))),
                        }],
                        ..Default::default()
                    });
                }
                core::socket_address::Protocol::Udp => {
                    let proxy = UdpProxyConfig {
                        stat_prefix: name.clone(),
                        route_specifier: Some(udp_proxy_config::RouteSpecifier::Matcher(Matcher {
                            on_no_match: Some(Box::new(matcher::OnMatch {
                                on_match: Some(matcher::on_match::OnMatch::Action(
                                    TypedExtensionConfig {
                                        name: "route".into(),
                                        typed_config: Some(pack(&Route {
                                            cluster: name.clone(),
                                        })),
                                    },
                                )),
                                ..Default::default()
                            })),
                            ..Default::default()
                        })),
                        ..Default::default()
                    };
                    listener.listener_filters.push(listener::ListenerFilter {
                        name: "envoy.filters.udp_listener.udp_proxy".into(),
                        config_type: Some(listener::listener_filter::ConfigType::TypedConfig(
                            pack(&proxy),
                        )),
                        ..Default::default()
                    });
                }
            }
            snapshot.clusters.insert(name.clone(), pack(&cluster));
            snapshot.endpoints.insert(name.clone(), pack(&assignment));
            snapshot.listeners.insert(name, pack(&listener));
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;

    pub fn service() -> Service {
        serde_json::from_value(json!({
            "metadata": {"name": "envoy", "namespace": "ingress"},
            "spec": {
                "type": "LoadBalancer", "loadBalancerClass": "noa.re/external-envoy",
                "ports": [
                    {"name": "https", "port": 443, "targetPort": 10443, "protocol": "TCP"},
                    {"name": "quic", "port": 443, "targetPort": 10443, "protocol": "UDP"}
                ]
            }
        }))
        .unwrap()
    }

    pub fn slice() -> EndpointSlice {
        serde_json::from_value(json!({
            "metadata": {"name": "envoy-1", "namespace": "ingress", "labels": {"kubernetes.io/service-name": "envoy"}},
            "addressType": "IPv4",
            "ports": [{"name": "https", "port": 10443, "protocol": "TCP"}, {"name": "quic", "port": 10443, "protocol": "UDP"}],
            "endpoints": [
                {"addresses": ["10.0.0.1"], "conditions": {"ready": true}},
                {"addresses": ["10.0.0.2"], "conditions": {"ready": false}},
                {"addresses": ["10.0.0.3"], "conditions": {"ready": true, "terminating": true}},
                {"addresses": ["10.0.0.4"]}
            ]
        })).unwrap()
    }

    fn render(services: &[Service], slices: &[EndpointSlice]) -> Snapshot {
        build(services, slices).unwrap()
    }

    #[test]
    fn generates_tcp_and_udp_listeners_clusters_and_ready_backends() {
        let snapshot = render(&[service()], &[slice()]);
        assert_eq!(snapshot.listeners.len(), 2);
        for protocol in ["tcp", "udp"] {
            let name = format!("ingress/envoy/{protocol}/443");
            let listener =
                listener::Listener::decode(snapshot.listeners[&name].value.as_slice()).unwrap();
            let core::address::Address::SocketAddress(socket) =
                listener.address.unwrap().address.unwrap()
            else {
                panic!()
            };
            assert_eq!(socket.address, "::");
            assert!(socket.ipv4_compat);
            if protocol == "tcp" {
                let listener::filter::ConfigType::TypedConfig(config) = listener.filter_chains[0]
                    .filters[0]
                    .config_type
                    .as_ref()
                    .unwrap()
                else {
                    panic!()
                };
                let proxy = TcpProxy::decode(config.value.as_slice()).unwrap();
                assert_eq!(
                    proxy.cluster_specifier,
                    Some(tcp_proxy::ClusterSpecifier::Cluster(name.clone()))
                );
            } else {
                let listener::listener_filter::ConfigType::TypedConfig(config) =
                    listener.listener_filters[0].config_type.as_ref().unwrap()
                else {
                    panic!()
                };
                let proxy = UdpProxyConfig::decode(config.value.as_slice()).unwrap();
                let udp_proxy_config::RouteSpecifier::Matcher(matcher) =
                    proxy.route_specifier.unwrap()
                else {
                    panic!()
                };
                let matcher::on_match::OnMatch::Action(action) =
                    matcher.on_no_match.unwrap().on_match.unwrap()
                else {
                    panic!()
                };
                let route = Route::decode(action.typed_config.unwrap().value.as_slice()).unwrap();
                assert_eq!(route.cluster, name);
            }
            let cluster =
                cluster::Cluster::decode(snapshot.clusters[&name].value.as_slice()).unwrap();
            assert_eq!(cluster.eds_cluster_config.unwrap().service_name, name);
            let assignment =
                endpoint::ClusterLoadAssignment::decode(snapshot.endpoints[&name].value.as_slice())
                    .unwrap();
            assert_eq!(assignment.endpoints[0].lb_endpoints.len(), 2);
            for endpoint in &assignment.endpoints[0].lb_endpoints {
                let endpoint::lb_endpoint::HostIdentifier::Endpoint(endpoint) =
                    endpoint.host_identifier.as_ref().unwrap()
                else {
                    panic!()
                };
                let core::address::Address::SocketAddress(socket) =
                    endpoint.address.as_ref().unwrap().address.as_ref().unwrap()
                else {
                    panic!()
                };
                assert!(matches!(socket.address.as_str(), "10.0.0.1" | "10.0.0.4"));
                assert_eq!(
                    socket.port_specifier,
                    Some(core::socket_address::PortSpecifier::PortValue(10443))
                );
            }
        }
    }

    #[test]
    fn selects_slice_namespace_service_port_name_and_protocol() {
        let service = service();
        let mut slices = vec![slice(); 4];
        slices[0].metadata.namespace = Some("wrong".into());
        slices[1]
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("kubernetes.io/service-name".into(), "wrong".into());
        slices[2]
            .ports
            .as_mut()
            .unwrap()
            .iter_mut()
            .for_each(|p| p.name = Some("wrong".into()));
        slices[3]
            .ports
            .as_mut()
            .unwrap()
            .retain(|p| p.protocol.as_deref() == Some("UDP"));
        let snapshot = render(&[service], &slices);
        let tcp = endpoint::ClusterLoadAssignment::decode(
            snapshot.endpoints["ingress/envoy/tcp/443"].value.as_slice(),
        )
        .unwrap();
        let udp = endpoint::ClusterLoadAssignment::decode(
            snapshot.endpoints["ingress/envoy/udp/443"].value.as_slice(),
        )
        .unwrap();
        assert!(tcp.endpoints[0].lb_endpoints.is_empty());
        assert_eq!(udp.endpoints[0].lb_endpoints.len(), 2);
    }

    #[test]
    fn deduplicates_addresses_and_resolves_ports_per_slice() {
        let service = service();
        let port = &service.spec.as_ref().unwrap().ports.as_ref().unwrap()[0];
        let mut second = slice();
        second.ports.as_mut().unwrap()[0].port = Some(11443);
        let addresses = backend_addresses(&service, port, &[slice(), slice(), second]).unwrap();
        assert_eq!(addresses.len(), 4);
        assert!(addresses.contains(&("10.0.0.1".parse().unwrap(), 11443)));
        let mut ipv6 = slice();
        ipv6.address_type = "IPv6".into();
        ipv6.endpoints.as_mut().unwrap().truncate(1);
        ipv6.endpoints.as_mut().unwrap()[0].addresses = vec!["fd00::1".into()];
        let addresses = backend_addresses(&service, port, &[slice(), ipv6]).unwrap();
        assert_eq!(addresses.len(), 3);
    }

    #[test]
    fn rejects_listener_collisions_and_unsupported_protocols() {
        let first = service();
        let mut second = service();
        second.metadata.name = Some("other".into());
        assert!(
            build([&first, &second], &[])
                .unwrap_err()
                .to_string()
                .contains("conflict")
        );
        second.spec.as_mut().unwrap().ports.as_mut().unwrap()[0].protocol = Some("SCTP".into());
        assert!(build([&second], &[]).is_err());
    }

    #[test]
    fn rejects_source_ranges_and_invalid_ports_instead_of_publishing_them() {
        let mut service = service();
        service.spec.as_mut().unwrap().load_balancer_source_ranges =
            Some(vec!["192.0.2.0/24".into()]);
        assert!(
            build([&service], &[])
                .unwrap_err()
                .to_string()
                .contains("loadBalancerSourceRanges")
        );
        for port in [-1, 0, 65536] {
            assert!(port_number(port).is_err());
        }
    }

    #[test]
    fn endpoint_changes_leave_listeners_and_clusters_unchanged() {
        let initial = render(&[service()], &[slice()]);
        let empty = render(&[service()], &[]);
        assert_eq!(initial.clusters, empty.clusters);
        assert_eq!(initial.listeners, empty.listeners);
        assert_ne!(initial.endpoints, empty.endpoints);
        assert_eq!(render(&[], &[]), Snapshot::default());
    }

    #[test]
    fn unnamed_service_port_matches_unnamed_endpoint_port() {
        let mut service = service();
        let ports = service.spec.as_mut().unwrap().ports.as_mut().unwrap();
        ports.truncate(1);
        ports[0].name = None;
        let mut slice = slice();
        let ports = slice.ports.as_mut().unwrap();
        ports.truncate(1);
        ports[0].name = Some(String::new());
        assert_eq!(
            backend_addresses(
                &service,
                &service.spec.as_ref().unwrap().ports.as_ref().unwrap()[0],
                &[slice]
            )
            .unwrap()
            .len(),
            2
        );
    }
}
