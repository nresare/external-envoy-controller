// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use tonic_reflection::server::Builder;

const ADS_SERVICE: &str = "envoy.service.discovery.v3.AggregatedDiscoveryService";

pub fn builder() -> Builder<'static> {
    Builder::configure()
        .register_encoded_file_descriptor_set(include_bytes!("../proto/xds-descriptors.bin"))
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_reflection::pb::v1alpha::FILE_DESCRIPTOR_SET)
        .include_reflection_service(false)
        // Imported files also describe services that this controller does not serve.
        .with_service_name(ADS_SERVICE)
        .with_service_name("grpc.health.v1.Health")
        .with_service_name("grpc.reflection.v1.ServerReflection")
        .with_service_name("grpc.reflection.v1alpha.ServerReflection")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{resources, xds};
    use envoy_types::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer;
    use prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot, watch};
    use tokio::time::timeout;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::Server;

    // Exercise both reflection protocols over gRPC, as used by inspection tools.
    macro_rules! reflection_test {
        ($name:ident, $version:ident) => {
            #[tokio::test]
            async fn $name() {
                use tonic_reflection::pb::$version::{
                    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
                    server_reflection_request::MessageRequest,
                    server_reflection_response::MessageResponse,
                };

                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (_snapshots, receiver) = watch::channel(None);
                let (stop, stopped) = oneshot::channel();
                let (_reporter, health_service) = crate::health::service().await;
                let server = tokio::spawn(
                    Server::builder()
                        .add_service(health_service)
                        .add_service(AggregatedDiscoveryServiceServer::new(xds::Ads {
                            snapshots: receiver,
                        }))
                        .add_service(builder().build_v1().unwrap())
                        .add_service(builder().build_v1alpha().unwrap())
                        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                            let _ = stopped.await;
                        }),
                );
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect_timeout(Duration::from_secs(5))
                    .connect()
                    .await
                    .unwrap();
                let mut client = ServerReflectionClient::new(channel);
                let symbols = [
                    ADS_SERVICE,
                    "grpc.health.v1.Health",
                    "envoy.config.cluster.v3.Cluster",
                    "envoy.config.endpoint.v3.ClusterLoadAssignment",
                    "envoy.config.listener.v3.Listener",
                    "envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy",
                    "envoy.extensions.filters.udp.udp_proxy.v3.UdpProxyConfig",
                    "envoy.extensions.filters.udp.udp_proxy.v3.Route",
                    "grpc.reflection.v1.ServerReflection",
                    "grpc.reflection.v1alpha.ServerReflection",
                ];
                let (requests, incoming) = mpsc::channel(1);
                let mut responses = client
                    .server_reflection_info(ReceiverStream::new(incoming))
                    .await
                    .unwrap()
                    .into_inner();
                requests
                    .send(ServerReflectionRequest {
                        message_request: Some(MessageRequest::ListServices(String::new())),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                let response = timeout(Duration::from_secs(5), responses.message())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let Some(MessageResponse::ListServicesResponse(services)) =
                    response.message_response
                else {
                    panic!("expected service list");
                };
                let mut services: Vec<_> = services.service.into_iter().map(|s| s.name).collect();
                services.sort();
                assert_eq!(
                    services,
                    [
                        ADS_SERVICE,
                        "grpc.health.v1.Health",
                        "grpc.reflection.v1.ServerReflection",
                        "grpc.reflection.v1alpha.ServerReflection",
                    ]
                );

                let mut files = BTreeMap::new();
                let mut pending: Vec<_> = symbols
                    .map(|symbol| MessageRequest::FileContainingSymbol(symbol.into()))
                    .into();
                // Fetch imports recursively, just as grpcurl does.
                while let Some(request) = pending.pop() {
                    if let MessageRequest::FileByFilename(name) = &request {
                        if files.contains_key(name) {
                            continue;
                        }
                    }
                    requests
                        .send(ServerReflectionRequest {
                            message_request: Some(request),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    let response = timeout(Duration::from_secs(5), responses.message())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    let Some(MessageResponse::FileDescriptorResponse(descriptors)) =
                        response.message_response
                    else {
                        panic!("expected descriptors");
                    };
                    for bytes in descriptors.file_descriptor_proto {
                        let file =
                            prost_types::FileDescriptorProto::decode(bytes.as_slice()).unwrap();
                        pending.extend(
                            file.dependency
                                .iter()
                                .cloned()
                                .map(MessageRequest::FileByFilename),
                        );
                        files.insert(file.name.clone().unwrap(), file);
                    }
                }
                let pool =
                    DescriptorPool::from_file_descriptor_set(prost_types::FileDescriptorSet {
                        file: files.into_values().collect(),
                    })
                    .unwrap();
                for symbol in symbols {
                    assert!(
                        pool.get_service_by_name(symbol).is_some()
                            || pool.get_message_by_name(symbol).is_some(),
                        "missing {symbol}"
                    );
                }
                let ads = pool.get_service_by_name(ADS_SERVICE).unwrap();
                assert!(
                    ads.methods()
                        .any(|method| method.name() == "StreamAggregatedResources"
                            && method.is_client_streaming()
                            && method.is_server_streaming())
                );

                // Resolve nested Any payloads from real TCP and UDP snapshots to JSON.
                let snapshot =
                    resources::build([&resources::tests::service()], &[resources::tests::slice()])
                        .unwrap();
                for resource in snapshot
                    .clusters
                    .values()
                    .chain(snapshot.endpoints.values())
                    .chain(snapshot.listeners.values())
                {
                    let name = resource
                        .type_url
                        .strip_prefix("type.googleapis.com/")
                        .unwrap();
                    let descriptor = pool.get_message_by_name(name).unwrap();
                    let message =
                        DynamicMessage::decode(descriptor, resource.value.as_slice()).unwrap();
                    let json = serde_json::to_value(message).unwrap();
                    assert!(json.is_object());
                }
                drop(requests);
                drop(responses);
                stop.send(()).unwrap();
                server.await.unwrap().unwrap();
            }
        };
    }

    reflection_test!(v1_describes_ads_and_decodes_resources, v1);
    reflection_test!(v1alpha_describes_ads_and_decodes_resources, v1alpha);
}
