// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use anyhow::{Context, bail};
use envoy_types::pb::envoy::config::{core::v3 as core, endpoint::v3 as endpoint};
use envoy_types::pb::google::protobuf::Any;
use prost::Message;
use serde_json::{Value, json};

use crate::resources::Snapshot;

fn resources(before: &BTreeMap<String, Any>, after: &BTreeMap<String, Any>) -> Value {
    json!({
        "added": after.keys().filter(|name| !before.contains_key(*name)).collect::<Vec<_>>(),
        "updated": after.iter().filter(|(name, value)| before.get(*name).is_some_and(|old| old != *value)).map(|(name, _)| name).collect::<Vec<_>>(),
        "removed": before.keys().filter(|name| !after.contains_key(*name)).collect::<Vec<_>>(),
    })
}

fn backends(resource: Option<&Any>) -> anyhow::Result<BTreeSet<SocketAddr>> {
    let Some(resource) = resource else {
        return Ok(BTreeSet::new());
    };
    let assignment = endpoint::ClusterLoadAssignment::decode(resource.value.as_slice())?;
    let mut addresses = BTreeSet::new();
    for locality in assignment.endpoints {
        for backend in locality.lb_endpoints {
            let Some(endpoint::lb_endpoint::HostIdentifier::Endpoint(endpoint)) =
                backend.host_identifier
            else {
                bail!("backend has no endpoint");
            };
            let Some(core::address::Address::SocketAddress(socket)) =
                endpoint.address.and_then(|address| address.address)
            else {
                bail!("backend has no socket address");
            };
            let Some(core::socket_address::PortSpecifier::PortValue(port)) = socket.port_specifier
            else {
                bail!("backend has no numeric port");
            };
            addresses.insert(SocketAddr::new(
                socket.address.parse()?,
                port.try_into().context("invalid backend port")?,
            ));
        }
    }
    Ok(addresses)
}

fn backend_changes(before: Option<&Any>, after: Option<&Any>) -> anyhow::Result<Value> {
    let before = backends(before)?;
    let after = backends(after)?;
    Ok(json!({
        "added": after.difference(&before).map(ToString::to_string).collect::<Vec<_>>(),
        "removed": before.difference(&after).map(ToString::to_string).collect::<Vec<_>>(),
    }))
}

pub fn between(previous: Option<&Snapshot>, snapshot: &Snapshot) -> Value {
    let empty = Snapshot::default();
    let previous = previous.unwrap_or(&empty);
    let names: BTreeSet<_> = previous
        .endpoints
        .keys()
        .chain(snapshot.endpoints.keys())
        .collect();
    let mut backends = BTreeMap::new();
    for name in names {
        let before = previous.endpoints.get(name);
        let after = snapshot.endpoints.get(name);
        if before != after {
            let changes = backend_changes(before, after)
                .unwrap_or_else(|error| json!({"error": error.to_string()}));
            backends.insert(name, changes);
        }
    }
    let mut changes = json!({
        "listeners": resources(&previous.listeners, &snapshot.listeners),
        "clusters": resources(&previous.clusters, &snapshot.clusters),
        "endpoints": resources(&previous.endpoints, &snapshot.endpoints),
        "backends": backends,
    });
    if previous.services != snapshot.services {
        let services = |snapshot: &Snapshot| {
            snapshot.services.iter().map(|(service, listeners)| json!({
            "namespace": service.namespace, "name": service.name, "uid": service.uid, "listeners": listeners,
        })).collect::<Vec<_>>()
        };
        changes["services"] = json!({"before": services(previous), "after": services(snapshot)});
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{
        build,
        tests::{service, slice},
    };

    #[test]
    fn initial_and_removed_resources_include_all_backends() {
        let snapshot = build([&service()], &[slice()]).unwrap();
        let initial = between(None, &snapshot);
        let removed = between(Some(&snapshot), &Snapshot::default());
        for kind in ["listeners", "clusters", "endpoints"] {
            assert_eq!(
                initial[kind]["added"],
                json!(["ingress/envoy/tcp/443", "ingress/envoy/udp/443"])
            );
            assert_eq!(removed[kind]["removed"], initial[kind]["added"]);
        }
        for name in snapshot.endpoints.keys() {
            assert_eq!(
                initial["backends"][name]["added"],
                json!(["10.0.0.1:10443", "10.0.0.4:10443"])
            );
            assert_eq!(
                removed["backends"][name]["removed"],
                initial["backends"][name]["added"]
            );
        }
    }

    #[test]
    fn pod_deletion_reports_removed_backends_with_unchanged_resource_counts() {
        let initial = build([&service()], &[slice()]).unwrap();
        let mut deleted = slice();
        deleted.endpoints.as_mut().unwrap().remove(0);
        let updated = build([&service()], &[deleted]).unwrap();
        let changes = between(Some(&initial), &updated);
        assert_eq!(
            changes["listeners"],
            resources(&initial.listeners, &initial.listeners)
        );
        assert_eq!(
            changes["clusters"],
            resources(&initial.clusters, &initial.clusters)
        );
        assert_eq!(
            changes["endpoints"]["updated"],
            json!(["ingress/envoy/tcp/443", "ingress/envoy/udp/443"])
        );
        for name in initial.endpoints.keys() {
            assert_eq!(
                changes["backends"][name],
                json!({"added": [], "removed": ["10.0.0.1:10443"]})
            );
        }
        assert!(changes.get("services").is_none());
    }

    #[test]
    fn backend_replacement_formats_ipv6_and_port_changes() {
        let initial = build([&service()], &[slice()]).unwrap();
        let mut replacement = slice();
        replacement.address_type = "IPv6".into();
        replacement.endpoints.as_mut().unwrap().truncate(1);
        replacement.endpoints.as_mut().unwrap()[0].addresses = vec!["fd00::1".into()];
        replacement.ports.as_mut().unwrap()[0].port = Some(11443);
        let updated = build([&service()], &[replacement]).unwrap();
        let changes = between(Some(&initial), &updated);
        assert_eq!(
            changes["backends"]["ingress/envoy/tcp/443"],
            json!({
                "added": ["[fd00::1]:11443"], "removed": ["10.0.0.1:10443", "10.0.0.4:10443"]
            })
        );
    }

    #[test]
    fn unchanged_and_empty_snapshots_have_no_changes() {
        for snapshot in [
            Snapshot::default(),
            build([&service()], &[slice()]).unwrap(),
        ] {
            let changes = between(Some(&snapshot), &snapshot);
            for kind in ["listeners", "clusters", "endpoints"] {
                assert_eq!(
                    changes[kind],
                    json!({"added": [], "updated": [], "removed": []})
                );
            }
            assert_eq!(changes["backends"], json!({}));
            assert!(changes.get("services").is_none());
        }
    }

    #[test]
    fn service_recreation_is_visible_even_with_identical_resources() {
        let mut service = service();
        service.metadata.uid = Some("old".into());
        let initial = build([&service], &[slice()]).unwrap();
        service.metadata.uid = Some("new".into());
        let updated = build([&service], &[slice()]).unwrap();
        let changes = between(Some(&initial), &updated);
        assert_eq!(changes["services"]["before"][0]["uid"], "old");
        assert_eq!(changes["services"]["after"][0]["uid"], "new");
        assert_eq!(changes["backends"], json!({}));
    }
}
