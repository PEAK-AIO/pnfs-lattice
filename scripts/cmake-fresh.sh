#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Configure (or re-configure) an out-of-tree CMake build directory.
# Removes a stale CMakeCache.txt when it was created for a different
# source or build path (common after rsyncing or tarring build/ from
# another host).
#
# Usage:
#   ./scripts/cmake-fresh.sh [build-dir] [extra cmake -B flags...]
#
# Examples:
#   ./scripts/cmake-fresh.sh build -DCMAKE_BUILD_TYPE=Release -DENABLE_RONDB=ON
#   ./scripts/cmake-fresh.sh build-rondb -DENABLE_RONDB=ON

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-build}"
shift || true

# shellcheck source=build-env.sh
source "${ROOT}/scripts/build-env.sh"

BUILD_PATH="${ROOT}/${BUILD_DIR}"
CACHE_FILE="${BUILD_PATH}/CMakeCache.txt"

cache_value() {
    local key="$1"
    local file="$2"
    local line

    [[ -f "${file}" ]] || return 1
    line="$(grep -m1 "^${key}:" "${file}" || true)"
    [[ -n "${line}" ]] || return 1
    printf '%s' "${line#*=}"
}

if [[ -f "${CACHE_FILE}" ]]; then
  cached_home="$(cache_value CMAKE_HOME_DIRECTORY "${CACHE_FILE}" || true)"
  cached_dir="$(cache_value CMAKE_CACHEFILE_DIR "${CACHE_FILE}" || true)"
  cached_dir="${cached_dir//$'\r'/}"

  stale=0
  if [[ -n "${cached_home}" && "${cached_home}" != "${ROOT}" ]]; then
    stale=1
  fi
  if [[ -n "${cached_dir}" && "${cached_dir}" != "${BUILD_PATH}" ]]; then
    stale=1
  fi

  if [[ "${stale}" -eq 1 ]]; then
    echo "cmake-fresh: removing stale ${BUILD_DIR}/ (was configured for" >&2
    echo "  source: ${cached_home:-?}" >&2
    echo "  build:  ${cached_dir:-?}" >&2
    echo "  now:    ${ROOT}" >&2
    rm -rf "${BUILD_PATH}"
  fi
fi

mkdir -p "${BUILD_PATH}" "${BUILD_TMP}"

# Re-export after sourcing build-env; CMake's compiler probe must not
# inherit a login-shell TMPDIR=/tmp.
export TMPDIR TEMP TMP
exec env TMPDIR="${TMPDIR}" TEMP="${TMPDIR}" TMP="${TMPDIR}" \
	cmake -S "${ROOT}" -B "${BUILD_PATH}" "$@"
