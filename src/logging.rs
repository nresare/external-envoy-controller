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
            info!(%peer, "gRPC client connected");
        })
}

pub fn request_span(request: &http::Request<()>) -> Span {
    let peer = request
        .extensions()
        .get::<TcpConnectInfo>()
        .and_then(|connection| connection.remote_addr)
        .map(|address| address.to_string());
    let span = info_span!("grpc", peer = peer.as_deref().unwrap_or("unknown"), rpc = %request.uri().path());
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
    use std::sync::{Arc, Mutex};
    use tokio::sync::{oneshot, watch};
    use tokio::time::{Duration, timeout};
    use tonic::transport::{Endpoint, Server};
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest,
    };

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
    async fn logs_connections_and_calls_across_grpc_services() {
        let buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
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
        HealthClient::new(channel.clone())
            .check(HealthCheckRequest {
                service: "liveness".into(),
            })
            .await
            .unwrap();
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
        let mut ads = AggregatedDiscoveryServiceClient::new(channel)
            .stream_aggregated_resources(tokio_stream::iter([DiscoveryRequest {
                type_url: crate::resources::CDS.into(),
                node: Some(envoy_types::pb::envoy::config::core::v3::Node {
                    id: "logging-test".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }]))
            .await
            .unwrap()
            .into_inner();
        assert!(ads.message().await.unwrap().is_some());
        drop(ads);
        stop.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("gRPC client connected"), "{logs}");
        for path in [
            "/grpc.health.v1.Health/Check",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources",
        ] {
            assert!(logs.contains(path), "missing {path}: {logs}");
        }
        assert!(logs.contains("peer=127.0.0.1:"), "{logs}");
        assert!(!logs.contains("peer=unknown"), "{logs}");
        assert_eq!(logs.matches("gRPC request").count(), 3, "{logs}");
    }
}
