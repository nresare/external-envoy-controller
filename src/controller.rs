// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use kube::runtime::{
    WatchStreamExt,
    watcher::{self, Event},
};
use kube::{Api, Client, ResourceExt};
use tokio::sync::watch;
use tracing::{info, info_span, warn};

use crate::resources::{self, Snapshot};
use crate::services::{Change, Services};

#[derive(Default)]
struct Slices {
    current: BTreeMap<(String, String), EndpointSlice>,
    initializing: BTreeMap<(String, String), EndpointSlice>,
}

impl Slices {
    fn log_event(&self, event: &Event<EndpointSlice>, services: &Services) {
        let slice = match event {
            Event::Init => {
                info_span!("kubernetes.endpointslice_sync")
                    .in_scope(|| info!("EndpointSlice watch synchronizing"));
                return;
            }
            Event::InitDone => {
                info_span!("kubernetes.endpointslice_sync").in_scope(|| {
                    info!(
                        slices = self.initializing.len() as i64,
                        "EndpointSlice watch synchronized"
                    )
                });
                return;
            }
            Event::Apply(slice) | Event::Delete(slice) => slice,
            Event::InitApply(_) => return,
        };
        let key = (slice.namespace().unwrap_or_default(), slice.name_any());
        let service = slice.labels().get("kubernetes.io/service-name");
        if !service.is_some_and(|name| {
            services
                .selected
                .contains_key(&(key.0.clone(), name.clone()))
        }) {
            return;
        }
        let deleted = matches!(event, Event::Delete(_));
        let previous = self.current.get(&key);
        if !deleted
            && previous.is_some_and(|old| {
                old.endpoints == slice.endpoints
                    && old.ports == slice.ports
                    && old.address_type == slice.address_type
            })
        {
            return;
        }
        let endpoints: Vec<_> = slice
            .endpoints
            .iter()
            .flatten()
            .map(|endpoint| {
                serde_json::json!({
                    "addresses": endpoint.addresses, "conditions": endpoint.conditions,
                })
            })
            .collect();
        let span = info_span!(
            "kubernetes.endpointslice_change",
            namespace = key.0,
            name = key.1,
            service,
            action = if deleted { "deleted" } else { "applied" },
            resource_version = slice.resource_version()
        );
        crate::telemetry::attributes(&span, "endpoints", &serde_json::json!(endpoints));
        crate::telemetry::attributes(&span, "ports", &serde_json::json!(slice.ports));
        let _entered = span.enter();
        info!(
            namespace = key.0, name = key.1, service,
            action = if deleted { "deleted" } else { "applied" },
            resource_version = slice.resource_version(),
            endpoints = %serde_json::json!(endpoints), ports = %serde_json::json!(slice.ports),
            "observed EndpointSlice topology change"
        );
    }

    fn handle(&mut self, event: Event<EndpointSlice>) -> bool {
        let key = |slice: &EndpointSlice| (slice.namespace().unwrap_or_default(), slice.name_any());
        match event {
            Event::Apply(slice) => {
                if self.current.get(&key(&slice)) == Some(&slice) {
                    return false;
                }
                self.current.insert(key(&slice), slice);
            }
            Event::Delete(slice) => return self.current.remove(&key(&slice)).is_some(),
            Event::Init => {
                self.initializing.clear();
                return false;
            }
            Event::InitApply(slice) => {
                self.initializing.insert(key(&slice), slice);
                return false;
            }
            Event::InitDone => {
                self.current = std::mem::take(&mut self.initializing);
            }
        }
        true
    }
}

fn publish(current: &mut Option<Arc<Snapshot>>, snapshot: Arc<Snapshot>) -> bool {
    if current.as_ref() == Some(&snapshot) {
        return false;
    }
    let changes = crate::snapshot_changes::between(current.as_deref(), &snapshot);
    let span = info_span!(
        "xds.publish",
        listeners = snapshot.listeners.len() as i64,
        clusters = snapshot.clusters.len() as i64,
        endpoints = snapshot.endpoints.len() as i64
    );
    crate::telemetry::attributes(&span, "changes", &changes);
    let _entered = span.enter();
    info!(
        listeners = snapshot.listeners.len() as i64,
        clusters = snapshot.clusters.len() as i64,
        endpoints = snapshot.endpoints.len() as i64,
        changes = %changes,
        "publishing xDS snapshot"
    );
    *current = Some(snapshot);
    true
}

pub async fn observe(
    client: Client,
    snapshots: watch::Sender<Option<Arc<Snapshot>>>,
) -> anyhow::Result<()> {
    let services = watcher::watcher(
        Api::<Service>::all(client.clone()),
        watcher::Config::default(),
    )
    .default_backoff();
    let slices = watcher::watcher(
        Api::<EndpointSlice>::all(client),
        watcher::Config::default(),
    )
    .default_backoff();
    futures::pin_mut!(services, slices);
    let mut service_state = Services::default();
    let mut slice_state = Slices::default();
    let mut services_ready = false;
    let mut slices_ready = false;
    loop {
        let changed = tokio::select! {
            event = services.next() => match event {
                Some(Ok(event)) => {
                    match &event {
                        Event::Init => info_span!("kubernetes.service_sync").in_scope(|| info!("Service watch synchronizing")),
                        Event::InitDone => { services_ready = true; info_span!("kubernetes.service_sync").in_scope(|| info!("Service watch synchronized")); }
                        _ => {}
                    }
                    let changes = service_state.handle(event);
                    let changed = !changes.is_empty();
                    for change in changes {
                        let (service, action, message) = match change {
                            Change::Observed(service) => (service, "observed", "observed matching Service"),
                            Change::Removed(service) => (service, "removed", "Service no longer observed"),
                        };
                        info_span!("kubernetes.service_change", namespace = service.namespace(), name = service.name_any(), action)
                            .in_scope(|| info!(namespace = service.namespace(), name = service.name_any(), "{message}"));
                    }
                    changed
                }
                Some(Err(error)) => { info_span!("kubernetes.service_watch_error").in_scope(|| warn!(%error, "Service watch failed; retrying with backoff")); false }
                None => anyhow::bail!("Service watch ended unexpectedly"),
            },
            event = slices.next() => match event {
                Some(Ok(event)) => {
                    if matches!(event, Event::InitDone) { slices_ready = true; }
                    slice_state.log_event(&event, &service_state);
                    slice_state.handle(event)
                }
                Some(Err(error)) => { info_span!("kubernetes.endpointslice_watch_error").in_scope(|| warn!(%error, "EndpointSlice watch failed; retrying with backoff")); false }
                None => anyhow::bail!("EndpointSlice watch ended unexpectedly"),
            }
        };
        if services_ready && slices_ready && (changed || snapshots.borrow().is_none()) {
            let slices = slice_state.current.values().cloned().collect::<Vec<_>>();
            match resources::build(service_state.selected.values(), &slices) {
                Ok(snapshot) => {
                    let snapshot = Arc::new(snapshot);
                    snapshots.send_if_modified(|current| publish(current, snapshot));
                }
                Err(error) => {
                    info_span!("xds.build_error").in_scope(|| warn!(%error, "cannot generate xDS snapshot; retaining last valid configuration"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::tests::slice;

    #[tokio::test(flavor = "current_thread")]
    async fn exports_structured_topology_and_backend_changes_over_otlp() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use tracing_subscriber::prelude::*;

        let collector = crate::telemetry::tests::Collector::start().await;
        let provider = collector.provider();
        let _dormant = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("topology-test")));
        let _guard = tracing::subscriber::set_default(subscriber);
        let service = crate::resources::tests::service();
        let mut services = Services::default();
        services.handle(Event::Apply(service.clone()));
        let mut slices = Slices::default();
        slices.handle(Event::Apply(slice()));
        let initial = resources::build([&service], &[slice()]).unwrap();
        let mut replacement = slice();
        replacement.endpoints.as_mut().unwrap()[0].addresses = vec!["10.244.5.66".into()];
        slices.log_event(&Event::Apply(replacement.clone()), &services);
        let latest = resources::build([&service], &[replacement]).unwrap();
        let mut current = Some(Arc::new(initial));
        assert!(publish(&mut current, Arc::new(latest)));
        let resources = collector.finish(provider).await;
        let spans: Vec<_> = resources
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .collect();
        let topology = spans
            .iter()
            .find(|span| span.name == "kubernetes.endpointslice_change")
            .unwrap();
        let ready = topology
            .attributes
            .iter()
            .find(|attribute| attribute.key == "endpoints.0.conditions.ready")
            .unwrap();
        assert_eq!(
            ready.value.as_ref().unwrap().value,
            Some(Value::BoolValue(true))
        );
        let snapshot = spans
            .iter()
            .find(|span| span.name == "xds.publish")
            .unwrap();
        for protocol in ["tcp", "udp"] {
            for (action, address) in [
                ("added", "10.244.5.66:10443"),
                ("removed", "10.0.0.1:10443"),
            ] {
                let key = format!("changes.backends.ingress/envoy/{protocol}/443.{action}");
                let attribute = snapshot
                    .attributes
                    .iter()
                    .find(|attribute| attribute.key == key)
                    .unwrap();
                let Some(Value::ArrayValue(array)) = &attribute.value.as_ref().unwrap().value
                else {
                    panic!("backend changes must be native arrays")
                };
                assert_eq!(array.values.len(), 1);
                assert_eq!(
                    array.values[0].value,
                    Some(Value::StringValue(address.into()))
                );
            }
        }
        assert!(
            snapshot
                .events
                .iter()
                .any(|event| event.name == "publishing xDS snapshot")
        );
    }

    #[tokio::test]
    #[ignore = "requires access to the current kubeconfig context; reads Services and EndpointSlices"]
    async fn live_kubernetes_snapshot() {
        let client = Client::try_default().await.unwrap();
        let (sender, mut receiver) = watch::channel(None);
        let observer = tokio::spawn(observe(client, sender));
        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            receiver.wait_for(Option::is_some),
        )
        .await
        .unwrap()
        .unwrap()
        .clone()
        .unwrap();
        observer.abort();
        assert_eq!(snapshot.clusters.len(), snapshot.listeners.len());
        assert_eq!(snapshot.clusters.len(), snapshot.endpoints.len());
        use prost::Message;
        for (name, resource) in &snapshot.endpoints {
            let assignment =
                envoy_types::pb::envoy::config::endpoint::v3::ClusterLoadAssignment::decode(
                    resource.value.as_slice(),
                )
                .unwrap();
            let count: usize = assignment
                .endpoints
                .iter()
                .map(|locality| locality.lb_endpoints.len())
                .sum();
            println!("{name}: {count} ready backends");
        }
    }

    #[test]
    fn slice_updates_deletions_and_completed_relists_replace_backends() {
        let mut state = Slices::default();
        let first = slice();
        state.handle(Event::Init);
        state.handle(Event::InitApply(first.clone()));
        assert!(state.current.is_empty());
        assert!(state.handle(Event::InitDone));
        assert_eq!(state.current.len(), 1);
        assert!(!state.handle(Event::Apply(first.clone())));
        let initial = resources::build(
            [&crate::resources::tests::service()],
            &state.current.values().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let mut replacement = first.clone();
        replacement.endpoints.as_mut().unwrap()[0].addresses = vec!["10.244.5.66".into()];
        assert!(state.handle(Event::Apply(replacement)));
        let updated = resources::build(
            [&crate::resources::tests::service()],
            &state.current.values().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let changes = crate::snapshot_changes::between(Some(&initial), &updated);
        for name in initial.endpoints.keys() {
            assert_eq!(
                changes["backends"][name],
                serde_json::json!({
                    "added": ["10.244.5.66:10443"], "removed": ["10.0.0.1:10443"]
                })
            );
        }

        let mut changed = first.clone();
        changed.endpoints.as_mut().unwrap()[0]
            .conditions
            .as_mut()
            .unwrap()
            .ready = Some(false);
        assert!(state.handle(Event::Apply(changed)));
        assert!(state.handle(Event::Delete(first.clone())));
        assert!(state.current.is_empty());
        state.handle(Event::Apply(first.clone()));
        state.handle(Event::Init);
        assert_eq!(state.current.len(), 1);
        state.handle(Event::InitDone);
        assert!(state.current.is_empty());
        state.handle(Event::Init);
        state.handle(Event::InitApply(first));
        state.handle(Event::Init);
        state.handle(Event::InitDone);
        assert!(state.current.is_empty());
    }
}
