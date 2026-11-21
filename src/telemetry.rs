// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::time::Duration;

use anyhow::Context;
use opentelemetry::{Array, Value, trace::TracerProvider};
use opentelemetry_otlp::{
    WithExportConfig, WithTonicConfig, tonic_types::transport::ClientTlsConfig,
};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tracing::{Metadata, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    filter::{LevelFilter, filter_fn},
    prelude::*,
};

use crate::cli::Args;

pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

pub fn init(args: &Args) -> anyhow::Result<Telemetry> {
    let provider = args.tracing_endpoint.as_deref().map(provider).transpose()?;
    let otel = provider.as_ref().map(|provider| {
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("external-envoy-controller"))
            .with_filter(filter_fn(export_enabled))
    });
    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer())
        .with(otel)
        .try_init()?;
    Ok(Telemetry { provider })
}

// This filter belongs only to the OpenTelemetry layer; stdout still records
// connection and probe activity. Health checks have a distinct static span name
// so filtering does not depend on inspecting the runtime RPC field.
pub fn export_enabled(metadata: &Metadata<'_>) -> bool {
    !matches!(metadata.name(), "grpc.connected" | "grpc.health_check")
}

fn provider(endpoint: &str) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(5));
    let exporter = if endpoint.starts_with("https://") {
        exporter.with_tls_config(ClientTlsConfig::new().with_native_roots())
    } else {
        exporter
    };
    let exporter = exporter
        .build()
        .context("cannot create OTLP gRPC trace exporter")?;
    Ok(SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("external-envoy-controller")
                .build(),
        )
        .with_batch_exporter(exporter)
        .build())
}

impl Telemetry {
    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(provider) = self.provider {
            // The tonic exporter needs Tokio to keep running during the flush.
            tokio::task::spawn_blocking(move || provider.shutdown()).await??;
        }
        Ok(())
    }
}

/// OTLP attributes support scalars and homogeneous arrays, not nested maps.
/// Keep objects searchable with dotted keys and retain address/name lists as arrays.
pub fn attributes(span: &Span, prefix: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                attributes(span, &format!("{prefix}.{key}"), value);
            }
        }
        serde_json::Value::Array(values) if values.iter().all(|value| value.is_string()) => {
            let values = values
                .iter()
                .map(|value| value.as_str().unwrap().to_owned().into())
                .collect();
            span.set_attribute(prefix.to_owned(), Value::Array(Array::String(values)));
        }
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                attributes(span, &format!("{prefix}.{index}"), value);
            }
        }
        serde_json::Value::String(value) => span.set_attribute(prefix.to_owned(), value.clone()),
        serde_json::Value::Bool(value) => span.set_attribute(prefix.to_owned(), *value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                span.set_attribute(prefix.to_owned(), value);
            } else if let Some(value) = value.as_f64() {
                span.set_attribute(prefix.to_owned(), value);
            }
        }
        serde_json::Value::Null => {}
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::trace::v1::{
            ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_server::{TraceService, TraceServiceServer},
        },
        trace::v1::ResourceSpans,
    };
    use tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
    };
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    struct Receiver(mpsc::UnboundedSender<ExportTraceServiceRequest>);

    #[tonic::async_trait]
    impl TraceService for Receiver {
        async fn export(
            &self,
            request: Request<ExportTraceServiceRequest>,
        ) -> Result<Response<ExportTraceServiceResponse>, Status> {
            self.0.send(request.into_inner()).unwrap();
            Ok(Response::new(ExportTraceServiceResponse::default()))
        }
    }

    pub struct Collector {
        endpoint: String,
        requests: mpsc::UnboundedReceiver<ExportTraceServiceRequest>,
        stop: oneshot::Sender<()>,
        server: JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl Collector {
        pub async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (requests, receiver) = mpsc::unbounded_channel();
            let (stop, stopped) = oneshot::channel();
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(TraceServiceServer::new(Receiver(requests)))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = stopped.await;
                    }),
            );
            Self {
                endpoint,
                requests: receiver,
                stop,
                server,
            }
        }

        pub fn provider(&self) -> SdkTracerProvider {
            provider(&self.endpoint).unwrap()
        }

        pub async fn finish(mut self, provider: SdkTracerProvider) -> Vec<ResourceSpans> {
            // Exercise the production shutdown path, including flushing the batch.
            Telemetry {
                provider: Some(provider),
            }
            .shutdown()
            .await
            .unwrap();
            let mut resources = Vec::new();
            while let Ok(request) = self.requests.try_recv() {
                resources.extend(request.resource_spans);
            }
            assert!(!resources.is_empty(), "collector received no OTLP exports");
            self.stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), self.server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            resources
        }
    }
}
