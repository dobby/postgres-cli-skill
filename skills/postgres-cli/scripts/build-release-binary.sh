#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
OUTPUT_BIN="${SCRIPT_DIR}/postgres-cli"

cd "${REPO_ROOT}"

echo "Building release binary..."
cargo build --release

cp "target/release/postgres-cli" "${OUTPUT_BIN}"
chmod +x "${OUTPUT_BIN}"

echo "Updated ${OUTPUT_BIN}"
file "${OUTPUT_BIN}"
