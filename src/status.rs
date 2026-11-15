// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures::StreamExt;
use k8s_openapi::api::core::v1::{LoadBalancerIngress, Service};
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{WatchStreamExt, watcher},
};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::{Duration, MissedTickBehavior};
use tracing::warn;

use crate::resources::{ServiceIdentity, service_listener_names};
use crate::services::Services;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Advertisement {
    pub addresses: BTreeSet<IpAddr>,
    pub listeners: BTreeSet<String>,
}

pub type Advertisements = BTreeMap<ServiceIdentity, Advertisement>;
type Connections = BTreeMap<u64, Advertisements>;

#[derive(Clone)]
pub struct Registry {
    connections: watch::Sender<Connections>,
    next_id: Arc<AtomicU64>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            connections: watch::channel(BTreeMap::new()).0,
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Registry {
    pub fn subscribe(&self) -> watch::Receiver<Connections> {
        self.connections.subscribe()
    }

    pub fn register(&self) -> Registration {
        Registration {
            registry: self.clone(),
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
        }
    }
}

pub struct Registration {
    registry: Registry,
    id: u64,
}

impl Registration {
    pub fn publish(&self, advertisements: &Advertisements) {
        self.registry.connections.send_if_modified(|connections| {
            if advertisements.is_empty() {
                return connections.remove(&self.id).is_some();
            }
            if connections.get(&self.id) == Some(advertisements) {
                return false;
            }
            connections.insert(self.id, advertisements.clone());
            true
        });
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry
            .connections
            .send_if_modified(|connections| connections.remove(&self.id).is_some());
    }
}

fn desired_ingress(service: &Service, connections: &Connections) -> Vec<LoadBalancerIngress> {
    let identity = ServiceIdentity::of(service);
    let listeners = service_listener_names(service);
    let addresses: BTreeSet<_> = connections
        .values()
        .filter_map(|advertisements| advertisements.get(&identity))
        .filter(|advertisement| !listeners.is_empty() && advertisement.listeners == listeners)
        .flat_map(|advertisement| advertisement.addresses.iter().copied())
        .collect();
    addresses
        .into_iter()
        .map(|address| LoadBalancerIngress {
            ip: Some(address.to_string()),
            ip_mode: Some("Proxy".into()),
            ..Default::default()
        })
        .collect()
}

fn status_patch(service: &Service, connections: &Connections) -> Option<Value> {
    if !crate::services::matches(service) {
        return None;
    }
    let desired = desired_ingress(service, connections);
    let current = service
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .and_then(|lb| lb.ingress.as_ref())
        .cloned()
        .unwrap_or_default();
    // Ignore ordering and unrelated ingress fields, but repair incorrect modes.
    if current.len() == desired.len()
        && current.iter().all(|entry| entry.hostname.is_none())
        && current
            .iter()
            .map(|entry| (&entry.ip, &entry.ip_mode))
            .collect::<BTreeSet<_>>()
            == desired
                .iter()
                .map(|entry| (&entry.ip, &entry.ip_mode))
                .collect::<BTreeSet<_>>()
    {
        return None;
    }
    Some(json!({
        "metadata": {"uid": service.uid()?, "resourceVersion": service.resource_version()?},
        "status": {"loadBalancer": {"ingress": desired}},
    }))
}

async fn reconcile_service(
    client: Client,
    service: &Service,
    connections: &Connections,
) -> Result<(), kube::Error> {
    let Some(patch) = status_patch(service, connections) else {
        return Ok(());
    };
    let api: Api<Service> = Api::namespaced(client, &service.namespace().unwrap_or_default());
    api.patch_status(
        &service.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

pub async fn reconcile(client: Client, registry: Registry) -> anyhow::Result<()> {
    let events = watcher::watcher(
        Api::<Service>::all(client.clone()),
        watcher::Config::default(),
    )
    .default_backoff();
    futures::pin_mut!(events);
    let mut services = Services::default();
    let mut initialized = false;
    let mut connections = registry.subscribe();
    let mut retry = tokio::time::interval(Duration::from_secs(5));
    retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = events.next() => match event {
                Some(Ok(event)) => {
                    if matches!(event, watcher::Event::InitDone) { initialized = true; }
                    services.handle(event);
                }
                Some(Err(error)) => { warn!(%error, "Service status watch failed; retrying"); continue; }
                None => anyhow::bail!("Service status watch ended unexpectedly"),
            },
            changed = connections.changed() => { changed?; },
            _ = retry.tick() => {},
        }
        if !initialized {
            continue;
        }
        let current = connections.borrow_and_update().clone();
        for service in services.selected.values() {
            if let Err(error) = reconcile_service(client.clone(), service, &current).await {
                // Conflicting versions and deleted Services are reconsidered after the next watch event.
                if !matches!(&error, kube::Error::Api(response) if response.code == 404 || response.code == 409)
                {
                    warn!(%error, namespace = service.namespace(), name = service.name_any(), "cannot update Service load-balancer status; retrying");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::client::Body;
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use tonic::codegen::{
        Service as HttpService,
        http::{Request, Response},
    };

    fn service() -> Service {
        let mut service = crate::resources::tests::service();
        service.metadata.uid = Some("uid-1".into());
        service.metadata.resource_version = Some("42".into());
        service
    }

    fn advertisement(service: &Service, addresses: &[&str]) -> Advertisements {
        [(
            ServiceIdentity::of(service),
            Advertisement {
                addresses: addresses.iter().map(|s| s.parse().unwrap()).collect(),
                listeners: service_listener_names(service),
            },
        )]
        .into()
    }

    #[test]
    fn active_connections_union_addresses_and_drop_without_losing_shared_ips() {
        let service = service();
        let registry = Registry::default();
        let first = registry.register();
        let second = registry.register();
        first.publish(&advertisement(&service, &["203.0.113.10", "2001:db8::10"]));
        second.publish(&advertisement(&service, &["203.0.113.10", "203.0.113.11"]));
        assert_eq!(
            desired_ingress(&service, &registry.connections.borrow()).len(),
            3
        );
        drop(first);
        assert_eq!(
            desired_ingress(&service, &registry.connections.borrow())
                .iter()
                .map(|i| i.ip.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["203.0.113.10", "203.0.113.11"]
        );
        drop(second);
        assert!(desired_ingress(&service, &registry.connections.borrow()).is_empty());
    }

    #[test]
    fn only_matching_service_incarnations_and_complete_port_sets_are_published() {
        let original = service();
        let connections = [(0, advertisement(&original, &["203.0.113.10"]))].into();
        let mut changed = original.clone();
        changed.metadata.uid = Some("uid-2".into());
        assert!(desired_ingress(&changed, &connections).is_empty());
        changed = original.clone();
        changed.spec.as_mut().unwrap().ports.as_mut().unwrap()[0].port = 8443;
        assert!(desired_ingress(&changed, &connections).is_empty());
        let mut unrelated = original.clone();
        unrelated.spec.as_mut().unwrap().load_balancer_class = Some("another/controller".into());
        assert!(status_patch(&unrelated, &connections).is_none());
    }

    #[test]
    fn patches_preserve_other_status_and_use_optimistic_concurrency() {
        let mut service = service();
        service.status = Some(serde_json::from_value(json!({
            "conditions": [{"type":"OtherController", "status":"True", "reason":"Ready", "lastTransitionTime":"2026-11-15T00:00:00Z"}],
            "loadBalancer": {"ingress":[{"ip":"203.0.113.99"}]},
        })).unwrap());
        let connections = [(0, advertisement(&service, &["203.0.113.10"]))].into();
        let patch = status_patch(&service, &connections).unwrap();
        assert_eq!(
            patch,
            json!({
                "metadata":{"uid":"uid-1","resourceVersion":"42"},
                "status":{"loadBalancer":{"ingress":[{"ip":"203.0.113.10","ipMode":"Proxy"}]}},
            })
        );
        service
            .status
            .as_mut()
            .unwrap()
            .load_balancer
            .as_mut()
            .unwrap()
            .ingress = Some(desired_ingress(&service, &connections));
        assert!(status_patch(&service, &connections).is_none());
        service
            .status
            .as_mut()
            .unwrap()
            .load_balancer
            .as_mut()
            .unwrap()
            .ingress
            .as_mut()
            .unwrap()[0]
            .ip_mode = Some("VIP".into());
        let repaired = status_patch(&service, &connections).unwrap();
        assert_eq!(
            repaired["status"]["loadBalancer"]["ingress"][0]["ipMode"],
            "Proxy"
        );
        service
            .status
            .as_mut()
            .unwrap()
            .load_balancer
            .as_mut()
            .unwrap()
            .ingress
            .as_mut()
            .unwrap()[0]
            .ip_mode = None;
        assert!(status_patch(&service, &connections).is_some());
        let withdrawn = status_patch(&service, &BTreeMap::new()).unwrap();
        assert_eq!(withdrawn["status"]["loadBalancer"]["ingress"], json!([]));
    }

    #[derive(Clone)]
    struct MockKubernetes {
        requests: Arc<Mutex<Vec<Value>>>,
        code: u16,
        service: Service,
    }

    impl HttpService<Request<Body>> for MockKubernetes {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<Body>) -> Self::Future {
            let mock = self.clone();
            Box::pin(async move {
                assert_eq!(request.method(), "PATCH");
                assert_eq!(
                    request.uri().path(),
                    "/api/v1/namespaces/ingress/services/envoy/status"
                );
                assert_eq!(
                    request.headers()["content-type"],
                    "application/merge-patch+json"
                );
                let bytes = request.into_body().collect_bytes().await.unwrap();
                mock.requests
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&bytes).unwrap());
                let body = if mock.code == 200 {
                    serde_json::to_vec(&mock.service).unwrap()
                } else {
                    serde_json::to_vec(&json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"resource version changed","code":mock.code})).unwrap()
                };
                Ok(Response::builder()
                    .status(mock.code)
                    .body(Body::from(body))
                    .unwrap())
            })
        }
    }

    #[tokio::test]
    async fn kubernetes_patch_targets_status_and_conflicts_are_retried_from_fresh_state() {
        let mut service = service();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = [(0, advertisement(&service, &["203.0.113.10"]))].into();
        let conflict = Client::new(
            MockKubernetes {
                requests: requests.clone(),
                code: 409,
                service: service.clone(),
            },
            "default",
        );
        assert!(
            matches!(reconcile_service(conflict, &service, &connections).await, Err(kube::Error::Api(response)) if response.code == 409)
        );
        service.metadata.resource_version = Some("43".into());
        let client = Client::new(
            MockKubernetes {
                requests: requests.clone(),
                code: 200,
                service: service.clone(),
            },
            "default",
        );
        reconcile_service(client, &service, &connections)
            .await
            .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["metadata"]["resourceVersion"], "42");
        assert_eq!(requests[1]["metadata"]["resourceVersion"], "43");
    }
}
