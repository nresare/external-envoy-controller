#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: The external-envoy-controller contributors
"""Extract reflection descriptors from the locked envoy-types dependency."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=ROOT,
    )
)
envoy = next(p for p in metadata["packages"] if p["name"] == "envoy-types")
source = Path(envoy["manifest_path"]).parent
subprocess.run(
    [
        "protoc",
        f"--descriptor_set_in={source / 'src/generated/types.bin'}",
        f"--descriptor_set_out={ROOT / 'proto/xds-descriptors.bin'}",
        "--include_imports",
        "envoy/service/discovery/v3/ads.proto",
        "envoy/config/cluster/v3/cluster.proto",
        "envoy/config/endpoint/v3/endpoint.proto",
        "envoy/config/listener/v3/listener.proto",
        "envoy/extensions/filters/network/tcp_proxy/v3/tcp_proxy.proto",
        "envoy/extensions/filters/udp/udp_proxy/v3/udp_proxy.proto",
        "envoy/extensions/filters/udp/udp_proxy/v3/route.proto",
    ],
    check=True,
)
print(f"Updated reflection descriptors from envoy-types {envoy['version']}")
