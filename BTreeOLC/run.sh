#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

BINARY="${BUILD_DIR}/BTreeOLCExample"

if [[ ! -f "${BINARY}" ]]; then
	echo "Binary not found. Building..."
	"${SCRIPT_DIR}/build.sh"
fi

"${BINARY}"
