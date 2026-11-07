// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

mod controller;
mod resources;
mod services;
mod xds;

use std::net::{IpAddr, SocketAddr};

use envoy_types::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer;
use kube::Client;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let listen: SocketAddr = std::env::var("XDS_LISTEN_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:18000".into())
        .parse()?;
    let bind: IpAddr = std::env::var("ENVOY_LISTENER_ADDRESS")
        .unwrap_or_else(|_| "::".into())
        .parse()?;
    let client = Client::try_default().await?;
    let (snapshots, receiver) = watch::channel(None);
    info!(
        load_balancer_class = services::LOAD_BALANCER_CLASS,
        "starting external-envoy-controller"
    );
    info!(%listen, listener_address = %bind, "serving xDS ADS");
    tokio::select! {
        result = controller::observe(client, snapshots, bind) => result?,
        result = Server::builder()
            .add_service(AggregatedDiscoveryServiceServer::new(xds::Ads { snapshots: receiver }))
            .serve(listen) => result?,
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
