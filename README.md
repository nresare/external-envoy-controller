# external-envoy-controller

A small controller that observes `Service` objects in the local Kubernetes
cluster and provides xDS configuration to an Envoy instance outside the
cluster that sets up TCP and UDP forwarding.

The initial use case is running a Kubernetes cluster on a private network
behind a router that has public IP connectivity and that can run an Envoy
proxy, but it might be useful in other similar situations.



## Development

Install Rust supporting edition 2024, including rustfmt and Clippy.
The controller uses the current kubeconfig context locally and ServiceAccount
credentials inside Kubernetes. It reads Services and EndpointSlices and patches
load-balancer status on Services selected by its load-balancer class.

```sh
cargo run --locked
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

An optional read-only smoke test builds a snapshot from your current cluster:

```sh
cargo test --locked live_kubernetes_snapshot -- --ignored --nocapture
```

### Buildkite

Set up the pipeline in Buildkite with the following step to generate it from
`.buildkite/pipelinegen.toml`:

```yaml
steps:
  - label: ":pipeline: Generate pipeline"
    command: uvx --from bktools --index https://repo.noa.re pipelinegen
```

## Container and Helm

```sh
docker build -t external-envoy-controller:0.1.0 .
helm lint helm/external-envoy-controller
helm template external-envoy-controller helm/external-envoy-controller
helm install external-envoy-controller helm/external-envoy-controller \
  --namespace external-envoy-controller --create-namespace
```

Make the image available to your cluster first; override `image.repository` and
`image.tag` for your registry. The chart grants cluster-wide `get`, `list`, and
`watch` for Services and EndpointSlices, and `patch` for `services/status`.
It creates an internal ClusterIP xDS Service on port 50051.
Release publishing is not configured.

## External Envoy

`examples/envoy-bootstrap.yaml` configures v3 state-of-the-world ADS for CDS, EDS,
and LDS. Listeners are sent after Envoy accepts their CDS/EDS dependencies.
Only the control-plane connection is static; traffic listeners, clusters,
and endpoints are supplied dynamically. Delta xDS is not supported. ACKs are
tracked per stream/type, NACK errors are logged, and a rejected version is not
repeated until configuration or subscriptions change.

The xDS transport is plaintext and unauthenticated. Keep it on a trusted private
connection or tunnel. For development, run this on the external Envoy host:

```sh
kubectl -n external-envoy-controller port-forward \
  service/external-envoy-controller 50051:50051
envoy -c examples/envoy-bootstrap.yaml
```

Keep port-forward running in one terminal and start Envoy in another.
Production connectivity and
authentication must be arranged separately. Envoy also needs routes to the pod
CIDRs, permission to bind low ports, and available listener addresses/ports.

Generated listeners cover only opted-in Services. Any other ports in an existing
Puppet configuration need corresponding opted-in Services before replacing that
configuration.

Set `node.metadata.listen_addresses` in the bootstrap to a non-empty list of
the IPv4/IPv6 addresses configured on the Envoy host. The example uses
documentation addresses that must be replaced. The controller creates one
listener per Service port and listen IP, named
`<namespace>/<service>/<protocol>/<port>/<ip>`. Cluster and endpoint resource
names remain `<namespace>/<service>/<protocol>/<port>`.

The address list is retained for the ADS stream, so subsequent requests can omit
the node metadata. Each Envoy receives its own listener bindings; new Service
ports use the same addresses automatically. Reconnect Envoy after changing its
bootstrap addresses. Missing metadata on an LDS subscription, invalid IPs,
wildcard/multicast addresses, and address changes within an existing stream are
rejected. CDS/EDS inspection does not require address metadata. Service
load-balancer status addresses do not select listener bindings.

### Service load-balancer status

For Services with `type: LoadBalancer` and
`loadBalancerClass: noa.re/external-envoy`, the controller reconciles
`status.loadBalancer.ingress` from the connected Envoys' `listen_addresses`.
Each ingress entry uses `ipMode: Proxy`, because Envoy forwards to pod addresses
and ports. Existing entries with a missing or different mode are corrected.
An address is published only after an LDS ACK covers every port of that Service
on that address. Inspecting CDS/EDS alone does not advertise an address.

Addresses are deduplicated across active ADS connections. A NACK retains the
last accepted configuration; closing a connection withdraws its addresses unless
another accepted connection still serves them. Service recreation or a changed
port set requires matching accepted listeners before publication. Kubernetes
write failures are logged and retried without interrupting xDS delivery. Updates
touch only load-balancer ingress and preserve other status fields.

Run one controller replica: connection state is held in this process, and
cross-replica status coordination is not implemented. On restart, stale status
is cleared until Envoys reconnect and acknowledge their configuration. The
Envoy Gateway controller can consume the backing Service addresses to update
Gateway status; this controller does not modify Gateway objects. An xDS ACK does
not verify external network reachability.

### Logging

At info level, the controller logs accepted TCP connections and each gRPC call
with the peer address and full RPC method path. This includes ADS, health, and
both reflection versions. Health probes produce a call log each time they run.
An ADS stream also logs its initial node identity, listen addresses, and first
subscription to each resource type. Routine ACKs stay at debug level; NACKs
remain warnings. Request headers and resource payloads are
not logged.

### Health checks

The standard `grpc.health.v1.Health` service shares the cleartext gRPC port
50051 with ADS and reflection. Kubernetes probes use health service names
`liveness` and `readiness`. Liveness reports `SERVING` while the process responds;
readiness reports `SERVING` once both Kubernetes watches have synchronized and
a valid snapshot has been published, including an empty snapshot. The empty
health service name reports overall readiness.

### Inspecting xDS

The xDS port also serves gRPC reflection (v1 and v1alpha), including descriptors
for the published resources and their TCP/UDP filter configurations. With the
port-forward running, use `grpcurl` without local protobuf files:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe \
  envoy.service.discovery.v3.AggregatedDiscoveryService
grpcurl -plaintext -d @ localhost:50051 \
  envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources
```

The last command opens an interactive stream. Enter this JSON to subscribe to
all clusters:

```json
{"node":{"id":"debug","cluster":"debug"},"typeUrl":"type.googleapis.com/envoy.config.cluster.v3.Cluster"}
```

Reflection supplies the schema; the client still manages xDS subscriptions and
ACKs. EDS subscriptions must name the desired clusters. Listeners are withheld
until the client accepts their CDS/EDS dependencies. Use Envoy's admin
`/config_dump?include_eds` endpoint to inspect configuration accepted by Envoy.

## License

MIT. The initial structure is adapted from `randomsecret`; its copyright notice
is retained in LICENSE.
