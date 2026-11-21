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
subscription to each resource type, including requested resource names. Kubernetes
watch synchronization and topology changes to EndpointSlices of selected Services
are logged, including addresses, readiness/termination conditions, and ports.
Each outgoing xDS response logs its peer, node ID, resource type, version, nonce,
and resource count. The first ACK of a newly accepted version is logged at info
level; duplicate ACKs stay at debug level. NACKs include the rejected version,
nonce, and error at warning level. Stream closure is logged even on cancellation,
with the last sent and accepted versions for each subscription.
Each snapshot publication includes a JSON `changes` field
listing added, updated, and removed listener, cluster, and endpoint resource
names. Backend changes list added and removed IP:port pairs per endpoint
resource, so pod replacement is visible even when resource counts stay the
same. Service identity or port-set changes include the previous and new Service
identities and listener names. The first snapshot is compared with an empty
configuration; unchanged snapshots are not published or logged. These logs
describe controller publication; they do not confirm that Envoy accepted it.
Request headers and full resource payloads are not logged.

### OpenTelemetry tracing

Tracing is optional; info-level stdout logging remains enabled. Pass an OTLP
gRPC collector endpoint on the command line:

```sh
cargo run --locked -- --tracing-endpoint http://tempo.tempo.svc:4317
```

The endpoint must be a collector base URL (usually port 4317), with an
`http://` or `https://` scheme and no signal path such as `/v1/traces`. HTTPS
uses the system's trusted certificate authorities. Omitting `--tracing-endpoint`
disables export. Invalid endpoints or unknown arguments fail startup. Collector
outages are handled asynchronously with a bounded batch queue; export failures
are logged and do not block reconciliation. Pending completed spans are flushed
on normal shutdown, including SIGTERM, with a bounded wait.

The deployment's `[[simple]]` block supplies the endpoint through `args`:

```toml
args = ["--tracing-endpoint", "http://tempo.tempo.svc:4317"]
```

For Helm, set `tracing.endpoint`; the chart passes it as the container argument:

```sh
helm upgrade --install external-envoy-controller helm/external-envoy-controller \
  --namespace external-envoy-controller --create-namespace \
  --set tracing.endpoint=http://tempo.tempo.svc:4317
```

Traces use `service.name=external-envoy-controller`. Short spans cover
EndpointSlice topology changes, snapshot publication, each ADS request and
response, and stream opening/closure. Updates are exported without waiting
for a long-lived ADS stream to close. The log messages appear as span events
with their fields as attributes: peer/node identity, resource type, version,
nonce, counts, acceptance, and rejection details.

Nested topology and snapshot state is also available as searchable span
attributes, rather than only JSON strings. For example,
`changes.backends.envoy-gateway-system/envoy-eg-d8c59e83/tcp/443.added` is a native
string array of IP:port pairs, `endpoints.0.conditions.ready` is a boolean, and
`resource_names` is a string array. Object fields use dotted keys; arrays of
objects use numeric indexes. An ACK/NACK can be matched to a response by
peer/node, resource type, version, and nonce. These spans describe controller
actions and Envoy's responses; they do not assert backend network reachability.

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

The example bootstrap enables Envoy's admin interface on `127.0.0.1:9901`.
For an existing Envoy, add the example's `admin` block to its bootstrap and
restart it if the interface is not already enabled. Run these commands on the
external Envoy host (or access the port through an SSH tunnel):

```sh
curl -fsS 'http://127.0.0.1:9901/config_dump?include_eds' | jq .
curl -fsS 'http://127.0.0.1:9901/clusters?format=json' | jq .
curl -fsS 'http://127.0.0.1:9901/listeners?format=json' | jq .
```

`config_dump` includes dynamic configuration versions and update timestamps;
`include_eds` includes backend addresses. `/clusters` shows discovered hosts
and their health. To compare configuration around a pod deletion, save dumps
before and after the event:

```sh
curl -fsS 'http://127.0.0.1:9901/config_dump?include_eds' | jq -S . > before.json
# After the backend change:
curl -fsS 'http://127.0.0.1:9901/config_dump?include_eds' | jq -S . > after.json
diff -u before.json after.json
```

### Diagnosing stale endpoints and disconnected ADS

Follow a pod replacement through the info logs: `observed EndpointSlice topology
change`, `publishing xDS snapshot` with old/new backend IP:port pairs,
`sending xDS response` for `ClusterLoadAssignment`, then `Envoy accepted xDS
configuration` with the same version and nonce. A snapshot publication alone
does not mean it was delivered or accepted. If publication appears without a
response, check for a connected ADS stream and an EDS subscription naming the
cluster. If the response has no ACK, look for a NACK or transport failure. An
ACK followed by a stale dump warrants checking that the dump comes from the
same Envoy node and that it has no other control-plane configuration.

`HEALTHY` in a host listing or an endpoint's configured health status is not an
ADS connection check. Without active health checks, hosts can remain healthy
while unreachable. Envoy retains its last accepted configuration when the
management server disconnects. Inspect the connection gauge and update counters
on the Envoy host:

```sh
curl -fsSG 'http://127.0.0.1:9901/stats' \
  --data-urlencode 'filter=control_plane.connected_state|update_success|update_failure|update_rejected|cx_connect_fail'
```

`control_plane.connected_state` is 1 for connected and 0 for disconnected. It
reflects disconnections Envoy has detected; a broken transport can remain
apparently connected until detected. The example bootstrap sends HTTP/2
keepalive probes every 30 seconds with a 5 second timeout, and the controller
also probes connections at those intervals. Apply the example's
`connection_keepalive` block to the static xDS cluster in an existing bootstrap
and restart Envoy to enable client-side detection. Server probes alone cannot
enable detection on an Envoy whose controller is already gone. Keepalives detect
transport failure; they do not verify endpoint freshness or make an intermediate
proxy's upstream connection healthy.

See Envoy's [management server documentation](https://www.envoyproxy.io/docs/envoy/latest/configuration/overview/mgmt_server)
and [cluster health-check configuration](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/cluster/v3/cluster.proto#envoy-v3-api-field-config-cluster-v3-cluster-health-checks).

Keep admin bound to localhost: it also exposes privileged operations.
See the [Envoy admin API documentation](https://www.envoyproxy.io/docs/envoy/latest/operations/admin).

## License

MIT. The initial structure is adapted from `randomsecret`; its copyright notice
is retained in LICENSE.
