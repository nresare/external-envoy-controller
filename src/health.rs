// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::sync::Arc;

use anyhow::Context;
use tokio::sync::watch;
use tonic_health::ServingStatus;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::server::{HealthReporter, health_reporter};

use crate::resources::Snapshot;

pub async fn service() -> (HealthReporter, HealthServer<impl Health>) {
    let (reporter, service) = health_reporter();
    reporter
        .set_service_status("liveness", ServingStatus::Serving)
        .await;
    reporter
        .set_service_status("readiness", ServingStatus::NotServing)
        .await;
    reporter
        .set_service_status("", ServingStatus::NotServing)
        .await;
    (reporter, service)
}

pub async fn observe(
    reporter: HealthReporter,
    mut snapshots: watch::Receiver<Option<Arc<Snapshot>>>,
) -> anyhow::Result<()> {
    loop {
        // An empty snapshot is valid once both Kubernetes watches synchronize.
        let status = if snapshots.borrow_and_update().is_some() {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        reporter.set_service_status("readiness", status).await;
        reporter.set_service_status("", status).await;
        snapshots
            .changed()
            .await
            .context("snapshot channel closed")?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Endpoint, Server};
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response, health_client::HealthClient,
    };

    #[tokio::test]
    async fn grpc_liveness_is_independent_of_snapshot_readiness() {
        let (reporter, service) = service().await;
        let (snapshots, receiver) = watch::channel(None);
        let monitor = tokio::spawn(observe(reporter, receiver));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);
        let live = client
            .check(HealthCheckRequest {
                service: "liveness".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            live.status,
            health_check_response::ServingStatus::Serving as i32
        );
        let overall = client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            overall.status,
            health_check_response::ServingStatus::NotServing as i32
        );
        let mut readiness = client
            .watch(HealthCheckRequest {
                service: "readiness".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let initial = timeout(Duration::from_secs(2), readiness.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            initial.status,
            health_check_response::ServingStatus::NotServing as i32
        );
        snapshots.send(Some(Arc::new(Snapshot::default()))).unwrap();
        let ready = timeout(Duration::from_secs(2), readiness.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            ready.status,
            health_check_response::ServingStatus::Serving as i32
        );
        // Withdrawing a snapshot changes readiness, without making the process unhealthy.
        snapshots.send(None).unwrap();
        let waiting = timeout(Duration::from_secs(2), readiness.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            waiting.status,
            health_check_response::ServingStatus::NotServing as i32
        );
        let live = client
            .check(HealthCheckRequest {
                service: "liveness".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            live.status,
            health_check_response::ServingStatus::Serving as i32
        );
        drop(readiness);
        stop.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(snapshots);
        assert!(
            timeout(Duration::from_secs(2), monitor)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
}
