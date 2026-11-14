# Reflection descriptors

`xds-descriptors.bin` contains the ADS, CDS, EDS, LDS, TCP proxy, and UDP proxy
protobuf descriptors and their imports, extracted from `envoy-types` 0.7.7.
They allow reflection clients to decode the nested `Any` messages published by
the controller. Only ADS and the reflection services are advertised as callable
services.

The descriptor set is checked in so normal builds do not require `protoc` or
access to protobuf source trees. When updating `envoy-types` or adding resource
types, regenerate it using the dependency selected by `Cargo.lock`:

```sh
python3 scripts/update-reflection-descriptors.py
```

Regeneration requires Cargo, Python 3, and `protoc`. The descriptors come from
[envoy-types](https://github.com/flemosr/envoy-types), distributed under the
Apache License 2.0; see `LICENSE.envoy-types`.
