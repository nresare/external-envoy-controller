// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::{ffi::OsString, path::Path};

use anyhow::{Context, bail};
use serde::Deserialize;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub tracing: Option<Tracing>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tracing {
    pub endpoint: String,
}

impl Config {
    pub fn load(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<Self> {
        let args: Vec<_> = args.into_iter().collect();
        match args.as_slice() {
            [] => Ok(Self::default()),
            [flag, path] if flag == "--config" => {
                let path = Path::new(path);
                let contents = std::fs::read_to_string(path)
                    .with_context(|| format!("cannot read config {}", path.display()))?;
                Self::parse(&contents).with_context(|| format!("invalid config {}", path.display()))
            }
            _ => bail!("usage: external-envoy-controller [--config PATH]"),
        }
    }

    fn parse(contents: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(contents)?;
        if let Some(tracing) = &config.tracing {
            let uri: tonic::codegen::http::Uri = tracing
                .endpoint
                .parse()
                .context("tracing.endpoint must be an HTTP(S) gRPC collector URL")?;
            anyhow::ensure!(
                matches!(uri.scheme_str(), Some("http" | "https"))
                    && uri.authority().is_some()
                    && !uri.host().unwrap_or_default().is_empty()
                    && uri.path_and_query().is_none_or(|path| path.as_str() == "/"),
                "tracing.endpoint must be an HTTP(S) gRPC collector URL without a path, e.g. http://otel-collector:4317"
            );
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_is_opt_in_and_requires_a_valid_grpc_endpoint() {
        assert!(Config::load([]).unwrap().tracing.is_none());
        assert!(Config::parse("").unwrap().tracing.is_none());
        assert!(
            Config::load([
                OsString::from("--config"),
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/controller.toml")
                    .into_os_string(),
            ])
            .unwrap()
            .tracing
            .is_some()
        );

        for endpoint in [
            "http://otel-collector:4317",
            "https://otel.example.com:4317",
            "http://[::1]:4317/",
        ] {
            let config = Config::parse(&format!("[tracing]\nendpoint = {endpoint:?}")).unwrap();
            assert_eq!(config.tracing.unwrap().endpoint, endpoint);
        }
        for input in [
            "[tracing]",
            "[tracing]\nendpoint = ''",
            "[tracing]\nendpoint = 'collector:4317'",
            "[tracing]\nendpoint = 'ftp://collector:4317'",
            "[tracing]\nendpoint = 'http://collector:4317/v1/traces'",
            "[tracing]\nendpont = 'http://collector:4317'",
            "[trace]\nendpoint = 'http://collector:4317'",
        ] {
            assert!(Config::parse(input).is_err(), "{input}");
        }
        assert!(Config::load([OsString::from("--config")]).is_err());
        assert!(Config::load([OsString::from("--unknown")]).is_err());
    }
}
