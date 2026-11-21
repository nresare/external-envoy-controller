// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::io;

use futures::{Stream, TryStreamExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::codegen::http;
use tonic::transport::server::{TcpConnectInfo, TcpIncoming};
use tracing::{Span, info, info_span};

pub fn incoming(listener: TcpListener) -> impl Stream<Item = io::Result<TcpStream>> {
    TcpIncoming::from(listener)
        .with_nodelay(Some(true))
        .inspect_ok(|stream| {
            let peer = stream
                .peer_addr()
                .map(|address| address.to_string())
                .unwrap_or_else(|_| "unknown".into());
            info_span!("grpc.connected", %peer).in_scope(|| info!(%peer, "gRPC client connected"));
        })
}

pub fn request_span(request: &http::Request<()>) -> Span {
    let peer = request
        .extensions()
        .get::<TcpConnectInfo>()
        .and_then(|connection| connection.remote_addr)
        .map(|address| address.to_string());
    let peer = peer.as_deref().unwrap_or("unknown");
    let rpc = request.uri().path();
    let span = if rpc == "/grpc.health.v1.Health/Check" {
        info_span!("grpc.health_check", peer, rpc = %rpc)
    } else {
        info_span!("grpc", peer, rpc = %rpc)
    };
    span.in_scope(|| info!("gRPC request"));
    span
}

#[cfg(test)]
mod tests {
    use super::*;
    use envoy_types::pb::envoy::service::discovery::v3::{
        DiscoveryRequest, aggregated_discovery_service_client::AggregatedDiscoveryServiceClient,
        aggregated_discovery_service_server::AggregatedDiscoveryServiceServer,
    };
    use opentelemetry::trace::TracerProvider;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, oneshot, watch};
    use tokio::time::{Duration, timeout};
    use tonic::transport::{Endpoint, Server};
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest,
    };
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn logs_grpc_calls_but_exports_only_useful_traces() {
        let buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
        let writer = buffer.clone();
        let collector = crate::telemetry::tests::Collector::start().await;
        let provider = collector.provider();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .without_time()
                    .with_writer(move || writer.clone()),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("logging-test"))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        crate::telemetry::export_enabled,
                    )),
            );
        // Keep two dispatches registered so parallel tests cannot cache this
        // subscriber's callsites as disabled on a thread without a subscriber.
        // https://github.com/tokio-rs/tracing/issues/3611
        let _dormant = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let _guard = tracing::subscriber::set_default(subscriber);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_reporter, health_service) = crate::health::service().await;
        let (_sender, snapshots) =
            watch::channel(Some(Arc::new(crate::resources::Snapshot::default())));
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .trace_fn(request_span)
                .add_service(health_service)
                .add_service(AggregatedDiscoveryServiceServer::new(crate::xds::Ads {
                    snapshots,
                    advertisements: crate::status::Registry::default(),
                }))
                .add_service(crate::reflection::builder().build_v1().unwrap())
                .serve_with_incoming_shutdown(incoming(listener), async {
                    let _ = stopped.await;
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap();
        for service in ["liveness", "readiness", ""] {
            HealthClient::new(channel.clone())
                .check(HealthCheckRequest {
                    service: service.into(),
                })
                .await
                .unwrap();
        }
        let mut reflection = ServerReflectionClient::new(channel.clone())
            .server_reflection_info(tokio_stream::iter([ServerReflectionRequest {
                message_request: Some(MessageRequest::ListServices(String::new())),
                ..Default::default()
            }]))
            .await
            .unwrap()
            .into_inner();
        assert!(reflection.message().await.unwrap().is_some());
        drop(reflection);
        let (requests, incoming) = mpsc::channel(8);
        let mut ads = AggregatedDiscoveryServiceClient::new(channel)
            .stream_aggregated_resources(tokio_stream::wrappers::ReceiverStream::new(incoming))
            .await
            .unwrap()
            .into_inner();
        requests
            .send(DiscoveryRequest {
                type_url: crate::resources::CDS.into(),
                node: Some(envoy_types::pb::envoy::config::core::v3::Node {
                    id: "logging-test".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let response = ads.message().await.unwrap().unwrap();
        requests
            .send(DiscoveryRequest {
                type_url: response.type_url,
                response_nonce: response.nonce,
                version_info: response.version_info,
                resource_names: vec!["missing".into()],
                ..Default::default()
            })
            .await
            .unwrap();
        let response = ads.message().await.unwrap().unwrap();
        requests
            .send(DiscoveryRequest {
                type_url: response.type_url,
                response_nonce: response.nonce,
                error_detail: Some(envoy_types::pb::google::rpc::Status {
                    code: 3,
                    message: "test rejection".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(ads.message().await.unwrap().is_some());
        drop(requests);
        assert!(ads.message().await.unwrap().is_none());
        drop(ads);
        stop.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let resources = collector.finish(provider).await;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        let resource = resources[0].resource.as_ref().unwrap();
        assert!(
            resource
                .attributes
                .iter()
                .any(|attribute| attribute.key == "service.name"
                    && attribute.value.as_ref().unwrap().value
                        == Some(Value::StringValue("external-envoy-controller".into())))
        );
        let spans: Vec<_> = resources
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .collect();
        for name in [
            "xds.stream_opened",
            "xds.request",
            "xds.response",
            "xds.stream_closed",
        ] {
            assert!(spans.iter().any(|span| span.name == name), "missing {name}");
        }
        assert!(
            spans
                .iter()
                .all(|span| !matches!(span.name.as_str(), "grpc.connected" | "grpc.health_check"))
        );
        assert!(
            spans
                .iter()
                .flat_map(|span| &span.events)
                .all(|event| event.name != "gRPC client connected")
        );
        assert!(
            spans
                .iter()
                .flat_map(|span| &span.attributes)
                .all(|attribute| attribute.key != "rpc"
                    || attribute.value.as_ref().unwrap().value
                        != Some(Value::StringValue("/grpc.health.v1.Health/Check".into())))
        );
        for rpc in [
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources",
        ] {
            assert!(
                spans.iter().any(|span| span.name == "grpc"
                    && span
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key == "rpc"
                            && attribute.value.as_ref().unwrap().value
                                == Some(Value::StringValue(rpc.into())))),
                "missing trace for {rpc}"
            );
        }
        let response = spans
            .iter()
            .find(|span| span.name == "xds.response")
            .unwrap();
        assert!(
            response
                .attributes
                .iter()
                .any(|attribute| attribute.key == "resources"
                    && attribute.value.as_ref().unwrap().value == Some(Value::IntValue(0)))
        );
        assert!(
            response
                .attributes
                .iter()
                .any(|attribute| attribute.key == "resource_names"
                    && matches!(
                        attribute.value.as_ref().unwrap().value,
                        Some(Value::ArrayValue(_))
                    ))
        );
        for event in [
            "Envoy accepted xDS configuration",
            "Envoy rejected xDS configuration",
            "ADS stream closed",
        ] {
            assert!(
                spans
                    .iter()
                    .flat_map(|span| &span.events)
                    .any(|candidate| candidate.name == event),
                "missing {event}"
            );
        }
        let nack = spans
            .iter()
            .flat_map(|span| &span.events)
            .find(|event| event.name == "Envoy rejected xDS configuration")
            .unwrap();
        assert!(
            nack.attributes
                .iter()
                .any(|attribute| attribute.key == "error_message"
                    && attribute.value.as_ref().unwrap().value
                        == Some(Value::StringValue("test rejection".into())))
        );
        assert!(
            nack.attributes
                .iter()
                .any(|attribute| attribute.key == "code"
                    && attribute.value.as_ref().unwrap().value == Some(Value::IntValue(3)))
        );
        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("gRPC client connected"), "{logs}");
        for message in [
            "ADS stream opened",
            "ADS subscription updated",
            "sending xDS response",
            "Envoy accepted xDS configuration",
            "Envoy rejected xDS configuration",
            "ADS stream closed",
            "logging-test",
            "test rejection",
            "nonce=",
            "version=",
        ] {
            assert!(logs.contains(message), "missing {message}: {logs}");
        }

        for path in [
            "/grpc.health.v1.Health/Check",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources",
        ] {
            assert!(logs.contains(path), "missing {path}: {logs}");
        }
        assert!(logs.contains("peer=127.0.0.1:"), "{logs}");
        assert!(!logs.contains("peer=unknown"), "{logs}");
        assert_eq!(logs.matches("gRPC request").count(), 5, "{logs}");
        assert_eq!(
            logs.matches("rpc=/grpc.health.v1.Health/Check").count(),
            3,
            "{logs}"
        );
    }
}
