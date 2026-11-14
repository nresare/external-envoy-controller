#!/usr/bin/env bash
set -euo pipefail

# Use the shared Rust checks, then add this repository's build and chart checks.
uvx --from bktools --index https://repo.noa.re pipelinegen
buildkite-agent pipeline upload .buildkite/pipeline.yml
