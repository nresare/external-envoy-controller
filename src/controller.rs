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
use tracing::{info, warn};

use crate::resources::{self, Snapshot};
use crate::services::{Change, Services};

#[derive(Default)]
struct Slices {
    current: BTreeMap<(String, String), EndpointSlice>,
    initializing: BTreeMap<(String, String), EndpointSlice>,
}

impl Slices {
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
                    if matches!(event, Event::InitDone) { services_ready = true; }
                    let changes = service_state.handle(event);
                    let changed = !changes.is_empty();
                    for change in changes {
                        match change {
                            Change::Observed(service) => info!(namespace = service.namespace(), name = service.name_any(), "observed matching Service"),
                            Change::Removed(service) => info!(namespace = service.namespace(), name = service.name_any(), "Service no longer observed"),
                        }
                    }
                    changed
                }
                Some(Err(error)) => { warn!(%error, "Service watch failed; retrying with backoff"); false }
                None => anyhow::bail!("Service watch ended unexpectedly"),
            },
            event = slices.next() => match event {
                Some(Ok(event)) => {
                    if matches!(event, Event::InitDone) { slices_ready = true; }
                    slice_state.handle(event)
                }
                Some(Err(error)) => { warn!(%error, "EndpointSlice watch failed; retrying with backoff"); false }
                None => anyhow::bail!("EndpointSlice watch ended unexpectedly"),
            }
        };
        if services_ready && slices_ready && (changed || snapshots.borrow().is_none()) {
            let slices = slice_state.current.values().cloned().collect::<Vec<_>>();
            match resources::build(service_state.selected.values(), &slices) {
                Ok(snapshot) => {
                    let snapshot = Arc::new(snapshot);
                    snapshots.send_if_modified(|current| {
                        if current.as_ref() == Some(&snapshot) {
                            return false;
                        }
                        info!(
                            listeners = snapshot.listeners.len(),
                            clusters = snapshot.clusters.len(),
                            endpoints = snapshot.endpoints.len(),
                            changes = %crate::snapshot_changes::between(current.as_deref(), &snapshot),
                            "publishing xDS snapshot"
                        );
                        *current = Some(snapshot);
                        true
                    });
                }
                Err(error) => {
                    warn!(%error, "cannot generate xDS snapshot; retaining last valid configuration")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::tests::slice;

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
