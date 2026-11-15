// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

mod controller;
mod health;
mod logging;
mod reflection;
mod resources;
mod services;
mod status;
mod xds;

use std::net::SocketAddr;

use envoy_types::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer;
use kube::Client;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let listen = SocketAddr::from(([0, 0, 0, 0], 50051));
    let client = Client::try_default().await?;
    let (snapshots, receiver) = watch::channel(None);
    let (health_reporter, health_service) = health::service().await;
    let health_snapshots = receiver.clone();
    let advertisements = status::Registry::default();
    let status_client = client.clone();
    let status_advertisements = advertisements.clone();
    let incoming = logging::incoming(tokio::net::TcpListener::bind(listen).await?);
    info!(
        load_balancer_class = services::LOAD_BALANCER_CLASS,
        "starting external-envoy-controller"
    );
    info!(%listen, "serving xDS ADS");
    tokio::select! {
        result = controller::observe(client, snapshots) => result?,
        result = health::observe(health_reporter, health_snapshots) => result?,
        result = status::reconcile(status_client, status_advertisements) => result?,
        result = Server::builder()
            .trace_fn(logging::request_span)
            .add_service(health_service)
            .add_service(AggregatedDiscoveryServiceServer::new(xds::Ads { snapshots: receiver, advertisements }))
            .add_service(reflection::builder().build_v1()?)
            .add_service(reflection::builder().build_v1alpha()?)
            .serve_with_incoming(incoming) => result?,
        result = shutdown_signal() => result?,
    }
    info!("external-envoy-controller terminated");
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
