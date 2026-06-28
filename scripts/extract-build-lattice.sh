#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Extract lattice.tar.gz, build pnfs-mds with RonDB, and install on this host.
#
# Usage:
#   ./scripts/extract-build-lattice.sh [path/to/lattice.tar.gz]
#
# Default tarball: ../lattice.tar.gz relative to the repo, or lattice.tar.gz
# in the current directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARBALL="${1:-}"

if [[ -z "${TARBALL}" ]]; then
  if [[ -f "${PWD}/lattice.tar.gz" ]]; then
    TARBALL="${PWD}/lattice.tar.gz"
  elif [[ -f "$(dirname "${REPO_ROOT}")/lattice.tar.gz" ]]; then
    TARBALL="$(dirname "${REPO_ROOT}")/lattice.tar.gz"
  else
    echo "usage: $0 path/to/lattice.tar.gz" >&2
    exit 1
  fi
fi

WORK_DIR="$(dirname "$(realpath "${TARBALL}")")"
ARCHIVE="$(basename "${TARBALL}")"

cd "${WORK_DIR}"
date

TOP="$(tar -tzf "${ARCHIVE}" | head -1 | cut -d/ -f1)"
rm -rf "${TOP}"
tar -zxvf "${ARCHIVE}"
cd "${TOP}/"

export CC="${CC:-gcc-13}"
export CXX="${CXX:-g++-13}"

ROOT="$(pwd)"

if [[ -x "./scripts/build-install-mds.sh" ]]; then
  exec ./scripts/build-install-mds.sh
fi

# Fallback when the tarball predates build-install-mds.sh / cmake-fresh.sh.
echo "scripts/build-install-mds.sh not found — using inline build" >&2

BUILD_DIR="${BUILD_DIR:-build}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
RONDB_ROOT="${RONDB_ROOT:-/opt/rondb}"

# shellcheck source=build-env.sh
source "${ROOT}/scripts/build-env.sh"

BUILD_PATH="${ROOT}/${BUILD_DIR}"
CACHE_FILE="${BUILD_PATH}/CMakeCache.txt"

if [[ -f "${CACHE_FILE}" ]]; then
  cached_home="$(grep -m1 '^CMAKE_HOME_DIRECTORY:' "${CACHE_FILE}" | cut -d= -f2 || true)"
  if [[ -n "${cached_home}" && "${cached_home}" != "${ROOT}" ]]; then
    echo "removing stale ${BUILD_DIR}/ (was ${cached_home})" >&2
    rm -rf "${BUILD_PATH}"
  fi
fi

rm -rf "${BUILD_PATH}"
cmake -S "${ROOT}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
  -DENABLE_RONDB=ON \
  -DRonDB_ROOT="${RONDB_ROOT}"
cmake --build "${BUILD_DIR}" -j"$(nproc)"

sudo install -m 755 scripts/pnfs-mds-top /usr/local/bin/
sudo install -m 755 "${BUILD_DIR}/src/mds/pnfs-mds" /usr/local/bin/pnfs-mds
sudo install -m 755 "${BUILD_DIR}/src/tools/mds-admin" /usr/local/bin/mds-admin
sudo systemctl stop pnfs-mds
sudo systemctl restart pnfs-mds
sudo systemctl status pnfs-mds --no-pager
