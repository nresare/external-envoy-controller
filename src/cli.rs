// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::ffi::OsString;

use anyhow::{Context, bail};

#[derive(Default)]
pub struct Args {
    pub tracing_endpoint: Option<String>,
}

impl Args {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<Self> {
        let args: Vec<_> = args.into_iter().collect();
        let endpoint = match args.as_slice() {
            [] => return Ok(Self::default()),
            [flag, endpoint] if flag == "--tracing-endpoint" => endpoint
                .to_str()
                .context("--tracing-endpoint must be valid UTF-8")?,
            _ => bail!("usage: external-envoy-controller [--tracing-endpoint URL]"),
        };
        let uri: tonic::codegen::http::Uri = endpoint
            .parse()
            .context("--tracing-endpoint must be an HTTP(S) gRPC collector URL")?;
        anyhow::ensure!(
            matches!(uri.scheme_str(), Some("http" | "https"))
                && uri.authority().is_some()
                && !uri.host().unwrap_or_default().is_empty()
                && uri.path_and_query().is_none_or(|path| path.as_str() == "/"),
            "--tracing-endpoint must be an HTTP(S) gRPC collector URL without a path, e.g. http://tempo.tempo.svc:4317"
        );
        Ok(Self {
            tracing_endpoint: Some(endpoint.to_owned()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_is_opt_in_and_accepts_http_https_and_ipv6_collectors() {
        assert!(Args::parse([]).unwrap().tracing_endpoint.is_none());
        for endpoint in [
            "http://tempo.tempo.svc:4317",
            "https://otel.example.com:4317",
            "http://[::1]:4317/",
        ] {
            let args =
                Args::parse([OsString::from("--tracing-endpoint"), endpoint.into()]).unwrap();
            assert_eq!(args.tracing_endpoint.as_deref(), Some(endpoint));
        }
    }

    #[test]
    fn rejects_invalid_endpoints_and_unrecognized_or_incomplete_arguments() {
        for endpoint in [
            "",
            "collector:4317",
            "ftp://collector:4317",
            "http://collector:4317/v1/traces",
            "http://collector:4317?query=1",
        ] {
            assert!(
                Args::parse([OsString::from("--tracing-endpoint"), endpoint.into()]).is_err(),
                "{endpoint}"
            );
        }
        for args in [
            vec!["--tracing-endpoint"],
            vec!["--unknown"],
            vec!["--config", "config.toml"],
            vec!["--tracing-endpoint", "http://tempo.tempo.svc:4317", "extra"],
            vec![
                "--tracing-endpoint",
                "http://tempo.tempo.svc:4317",
                "--tracing-endpoint",
                "http://other:4317",
            ],
        ] {
            assert!(
                Args::parse(args.iter().map(OsString::from)).is_err(),
                "{args:?}"
            );
        }
    }
}
