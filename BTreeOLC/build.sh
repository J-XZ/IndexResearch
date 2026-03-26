#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
HOST_ARCH="$(uname -m)"
HOST_OS="$(uname -s)"
SDK_PATH=""
CCACHE_BIN="$(command -v ccache || true)"
CCACHE_DIR_PATH="${SCRIPT_DIR}/.ccache"

CONFIG_ARGS=(
  -DCMAKE_BUILD_TYPE=Debug
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
)

if [[ -n "${CCACHE_BIN}" ]]; then
  export CCACHE_DIR="${CCACHE_DIR_PATH}"
  mkdir -p "${CCACHE_DIR}"
  CONFIG_ARGS+=(-DCMAKE_C_COMPILER_LAUNCHER="${CCACHE_BIN}")
  CONFIG_ARGS+=(-DCMAKE_CXX_COMPILER_LAUNCHER="${CCACHE_BIN}")
fi

if [[ "${HOST_OS}" == "Darwin" ]]; then
  SDK_PATH="$(xcrun --show-sdk-path)"
  CONFIG_ARGS+=(-DCMAKE_OSX_ARCHITECTURES="${HOST_ARCH}")
  CONFIG_ARGS+=(-DCMAKE_OSX_SYSROOT="${SDK_PATH}")
fi

if [[ -f "${BUILD_DIR}/CMakeCache.txt" ]] && [[ "${HOST_OS}" == "Darwin" ]]; then
  if ! grep -q "CMAKE_OSX_ARCHITECTURES:.*=${HOST_ARCH}" "${BUILD_DIR}/CMakeCache.txt" || \
     ! grep -q "CMAKE_OSX_SYSROOT:.*=${SDK_PATH}" "${BUILD_DIR}/CMakeCache.txt"; then
    rm -rf "${BUILD_DIR}"
  fi
fi

cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}" "${CONFIG_ARGS[@]}"

cp "${BUILD_DIR}/compile_commands.json" "${SCRIPT_DIR}/compile_commands.json"

cmake --build "${BUILD_DIR}" --config Debug
