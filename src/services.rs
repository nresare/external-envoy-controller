// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: The external-envoy-controller contributors

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use kube::runtime::watcher;

pub const LOAD_BALANCER_CLASS: &str = "noa.re/external-envoy";

type ServiceKey = (String, String);

#[derive(Default)]
pub struct Services {
    pub selected: BTreeMap<ServiceKey, Service>,
    initializing: BTreeMap<ServiceKey, Service>,
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Observed(Service),
    Removed(Service),
}

pub(crate) fn matches(service: &Service) -> bool {
    service.spec.as_ref().is_some_and(|spec| {
        spec.type_.as_deref() == Some("LoadBalancer")
            && spec.load_balancer_class.as_deref() == Some(LOAD_BALANCER_CLASS)
    })
}

fn key(service: &Service) -> ServiceKey {
    (service.namespace().unwrap_or_default(), service.name_any())
}

impl Services {
    fn apply(&mut self, service: Service, changes: &mut Vec<Change>) {
        let key = key(&service);
        if matches(&service) {
            if self.selected.get(&key) != Some(&service) {
                self.selected.insert(key, service.clone());
                changes.push(Change::Observed(service));
            }
        } else if let Some(previous) = self.selected.remove(&key) {
            changes.push(Change::Removed(previous));
        }
    }

    pub fn handle(&mut self, event: watcher::Event<Service>) -> Vec<Change> {
        use watcher::Event;
        let mut changes = Vec::new();
        match event {
            Event::Apply(service) => self.apply(service, &mut changes),
            Event::Delete(service) => {
                if let Some(previous) = self.selected.remove(&key(&service)) {
                    changes.push(Change::Removed(previous));
                }
            }
            // Keep the previous snapshot until a complete relist is available.
            Event::Init => self.initializing.clear(),
            Event::InitApply(service) => {
                if matches(&service) {
                    self.initializing.insert(key(&service), service);
                }
            }
            Event::InitDone => {
                let snapshot = std::mem::take(&mut self.initializing);
                self.selected.retain(|key, service| {
                    if snapshot.contains_key(key) {
                        true
                    } else {
                        changes.push(Change::Removed(service.clone()));
                        false
                    }
                });
                for service in snapshot.into_values() {
                    self.apply(service, &mut changes);
                }
            }
        }
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{ServicePort, ServiceSpec};
    use kube::api::ObjectMeta;
    use watcher::Event;

    fn service(name: &str) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ingress".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".into()),
                load_balancer_class: Some(LOAD_BALANCER_CLASS.into()),
                ports: Some(vec![ServicePort {
                    port: 443,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn selects_only_the_exact_type_and_class() {
        let selected = service("envoy");
        assert!(matches(&selected));
        for type_ in [None, Some("ClusterIP"), Some("NodePort")] {
            let mut other = selected.clone();
            other.spec.as_mut().unwrap().type_ = type_.map(str::to_owned);
            assert!(!matches(&other));
        }
        for class in [
            None,
            Some("other/controller"),
            Some("noa.re/external-envoy-extra"),
        ] {
            let mut other = selected.clone();
            other.spec.as_mut().unwrap().load_balancer_class = class.map(str::to_owned);
            assert!(!matches(&other));
        }
        assert!(!matches(&Service::default()));
    }

    #[test]
    fn observes_initial_services_and_port_updates() {
        let mut state = Services::default();
        let mut envoy = service("envoy");
        state.handle(Event::Init);
        assert!(state.handle(Event::InitApply(envoy.clone())).is_empty());
        assert_eq!(
            state.handle(Event::InitDone),
            vec![Change::Observed(envoy.clone())]
        );
        assert!(state.handle(Event::Apply(envoy.clone())).is_empty());
        envoy.spec.as_mut().unwrap().ports.as_mut().unwrap()[0].port = 8443;
        assert_eq!(
            state.handle(Event::Apply(envoy.clone())),
            vec![Change::Observed(envoy)]
        );
    }

    #[test]
    fn removes_deleted_or_deselected_services() {
        for delete in [true, false] {
            let mut state = Services::default();
            let envoy = service("envoy");
            state.handle(Event::Apply(envoy.clone()));
            let event = if delete {
                Event::Delete(envoy.clone())
            } else {
                let mut changed = envoy.clone();
                changed.spec.as_mut().unwrap().type_ = Some("ClusterIP".into());
                changed.spec.as_mut().unwrap().load_balancer_class = None;
                Event::Apply(changed)
            };
            assert_eq!(state.handle(event), vec![Change::Removed(envoy)]);
            assert!(state.selected.is_empty());
        }
    }

    #[test]
    fn relist_removes_missing_services_only_when_complete() {
        let mut state = Services::default();
        let envoy = service("envoy");
        state.handle(Event::Apply(envoy.clone()));
        state.handle(Event::Init);
        assert_eq!(state.selected.len(), 1);
        assert_eq!(state.handle(Event::InitDone), vec![Change::Removed(envoy)]);
    }

    #[test]
    fn interrupted_relist_does_not_retain_partial_results() {
        let mut state = Services::default();
        state.handle(Event::Init);
        state.handle(Event::InitApply(service("stale")));
        state.handle(Event::Init);
        let envoy = service("envoy");
        state.handle(Event::InitApply(envoy.clone()));
        assert_eq!(state.handle(Event::InitDone), vec![Change::Observed(envoy)]);
        assert_eq!(state.selected.len(), 1);
    }

    #[test]
    fn namespaces_are_distinct_and_unrelated_events_are_ignored() {
        let mut state = Services::default();
        let envoy = service("envoy");
        let mut other = envoy.clone();
        other.metadata.namespace = Some("other".into());
        state.handle(Event::Apply(envoy.clone()));
        state.handle(Event::Apply(other));
        assert_eq!(state.selected.len(), 2);
        state.handle(Event::Delete(envoy));
        assert_eq!(state.selected.len(), 1);
        let mut unrelated = service("unrelated");
        unrelated.spec.as_mut().unwrap().load_balancer_class = None;
        assert!(state.handle(Event::Apply(unrelated.clone())).is_empty());
        assert!(state.handle(Event::Delete(unrelated)).is_empty());
    }
}
