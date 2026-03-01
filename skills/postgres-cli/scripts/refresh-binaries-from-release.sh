#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/refresh-binaries-from-release.sh <tag>
# Example:
#   scripts/refresh-binaries-from-release.sh v0.2.0

TAG="${1:-}"
if [[ -z "${TAG}" ]]; then
  echo "Usage: $0 <tag>" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI (gh) is required." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BIN_DIR="${SCRIPT_DIR}/bin"

mkdir -p "${BIN_DIR}"

ARTIFACTS=(
  "postgres-cli-darwin-arm64"
  "postgres-cli-linux-x86_64"
  "postgres-cli-windows-x86_64.exe"
  "checksums.txt"
)

cd "${REPO_ROOT}"
for name in "${ARTIFACTS[@]}"; do
  echo "Downloading ${name} from release ${TAG}..."
  gh release download "${TAG}" --pattern "${name}" --dir "${BIN_DIR}"
  chmod +x "${BIN_DIR}/${name}" || true
done

echo "Verifying checksums..."
if [[ -f "${BIN_DIR}/checksums.txt" ]]; then
  (cd "${BIN_DIR}" && shasum -a 256 -c checksums.txt)
else
  echo "checksums.txt not present in bin directory; skipping checksum verification."
fi

echo "Done. Updated binaries in ${BIN_DIR}"
