#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BIN_DIR="${SCRIPT_DIR}/bin"

mkdir -p "${BIN_DIR}"

OS="$(uname -s)"
ARCH="$(uname -m)"

TARGET=""
OUT_NAME=""

case "${OS}:${ARCH}" in
  Darwin:arm64)
    TARGET="aarch64-apple-darwin"
    OUT_NAME="postgres-cli-darwin-arm64"
    ;;
  Linux:x86_64)
    TARGET="x86_64-unknown-linux-gnu"
    OUT_NAME="postgres-cli-linux-x86_64"
    ;;
  MINGW*:x86_64|MSYS*:x86_64|CYGWIN*:x86_64)
    TARGET="x86_64-pc-windows-msvc"
    OUT_NAME="postgres-cli-windows-x86_64.exe"
    ;;
  *)
    echo "Unsupported host ${OS}/${ARCH} for local release build." >&2
    exit 1
    ;;
esac

cd "${REPO_ROOT}"
echo "Building release binary for ${TARGET}..."
rustup target add "${TARGET}" >/dev/null 2>&1 || true
cargo build --release --target "${TARGET}"

SRC="target/${TARGET}/release/postgres-cli"
if [[ "${OUT_NAME}" == *.exe ]]; then
  SRC="${SRC}.exe"
fi

cp "${SRC}" "${BIN_DIR}/${OUT_NAME}"
chmod +x "${BIN_DIR}/${OUT_NAME}" || true

echo "Updated ${BIN_DIR}/${OUT_NAME}"
file "${BIN_DIR}/${OUT_NAME}" || true
