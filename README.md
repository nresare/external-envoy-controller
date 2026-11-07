# external-envoy-controller

A Kubernetes xDS control plane for standalone Envoy proxies outside the cluster.
The target environment uses Cilium and BGP routing so that the external Linux
gateway can reach pod IPs directly.

## Behavior

The controller watches Services across all namespaces and selects those with
`spec.type: LoadBalancer` and `spec.loadBalancerClass: noa.re/external-envoy`.
For each selected TCP or UDP Service port, it generates:

- An LDS listener bound to the Service port on the configured gateway address.
- A CDS cluster using EDS and round-robin load balancing.
- An EDS assignment containing matching EndpointSlice addresses and endpoint ports.

TCP traffic, including TLS, passes through unchanged. UDP uses Envoy's UDP proxy
with its default session behavior. HTTP routing and TLS termination remain the
responsibility of upstream applications or an in-cluster gateway.

EndpointSlices are matched by namespace, `kubernetes.io/service-name`, port name,
and protocol. Endpoint port numbers are taken from the slices, supporting named
Service target ports and different backend ports across slices. Unknown readiness
is treated as ready, explicit unready endpoints and terminating endpoints are
excluded, and duplicate address/port pairs are removed. IPv4 and IPv6 addresses
are supported; FQDN slices are ignored.

Resources are named `<namespace>/<service>/<tcp|udp>/<service-port>`. Service
updates, deletion, and class/type changes update or withdraw resources. Empty
backend sets produce empty EDS assignments, retaining the listener. Watch failures
retry with backoff and relists refresh state. Initial configuration waits for both
watches to synchronize. Endpoint-only changes do not change CDS/LDS versions.

All clients currently receive the same configuration. Two Services requesting the
same protocol and listener port conflict; TCP and UDP on the same port are valid.
A conflict, unsupported protocol (such as SCTP), configured
`loadBalancerSourceRanges`, or invalid endpoint/port leaves
the last valid snapshot in place and logs the error. Existing configuration can
therefore remain until the error is fixed.

## Development

Install Rust supporting edition 2024, including rustfmt and Clippy.
The controller uses the current kubeconfig context locally and ServiceAccount
credentials inside Kubernetes. It only reads Kubernetes resources.

```sh
RUST_LOG=info cargo run --locked
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

An optional read-only smoke test builds a snapshot from your current cluster:

```sh
cargo test --locked live_kubernetes_snapshot -- --ignored --nocapture
```

Configuration:

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `RUST_LOG` | `info` | Logging verbosity |
| `XDS_LISTEN_ADDRESS` | `127.0.0.1:18000` | Control plane gRPC bind address |
| `ENVOY_LISTENER_ADDRESS` | `::` | Bind address for generated listeners on external Envoy |

The default listener address is dual-stack `::` with IPv4 compatibility, matching
a Linux gateway with IPv6 enabled. Set an explicit IPv4 or IPv6 address to bind
only that address. The bind address is shared across Services; per-Service bind
addresses and multiple Envoy configurations are not implemented. Service
load-balancer status, source address preservation, session affinity, and traffic
policies are not reconciled.

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
`watch` for Services and EndpointSlices and creates an internal ClusterIP xDS
Service. `xds.port` defaults to 18000; `envoyListenerAddress` defaults to `::`.
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
  service/external-envoy-controller 18000:18000
envoy -c examples/envoy-bootstrap.yaml
```

Keep port-forward running in one terminal and start Envoy in another.
Production connectivity and
authentication must be arranged separately. Envoy also needs routes to the pod
CIDRs, permission to bind low ports, and available listener addresses/ports.

Generated listeners cover only opted-in Services. Any other ports in an existing
Puppet configuration need corresponding opted-in Services before replacing that
configuration. Service load-balancer IPs do not select the listener bind address.

## License

MIT. The initial structure is adapted from `randomsecret`; its copyright notice
is retained in LICENSE.
