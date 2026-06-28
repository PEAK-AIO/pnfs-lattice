#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Build pnfs-mds with RonDB and install binaries on this host.
# Matches the common MDS build-machine workflow (extract tarball,
# compile, restart systemd service).
#
# Usage:
#   cd pnfs-lattice
#   export CC=gcc-13 CXX=g++-13   # optional
#   ./scripts/build-install-mds.sh
#
# Environment overrides:
#   RONDB_ROOT=/opt/rondb
#   BUILD_DIR=build
#   CMAKE_BUILD_TYPE=Release
#   SKIP_INSTALL=1          # build only, do not copy binaries or restart
#   SKIP_SERVICE=1          # install binaries but do not touch systemd

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
RONDB_ROOT="${RONDB_ROOT:-/opt/rondb}"

# shellcheck source=build-env.sh
source "${ROOT}/scripts/build-env.sh"

cd "${ROOT}"

"${ROOT}/scripts/cmake-fresh.sh" "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
  -DENABLE_RONDB=ON \
  -DRonDB_ROOT="${RONDB_ROOT}"

cmake --build "${BUILD_DIR}" -j"$(nproc)"

if [[ "${SKIP_INSTALL:-0}" -eq 1 ]]; then
  exit 0
fi

sudo install -m 755 "${ROOT}/scripts/pnfs-mds-top" /usr/local/bin/
sudo install -m 755 "${BUILD_DIR}/src/mds/pnfs-mds" /usr/local/bin/pnfs-mds
sudo install -m 755 "${BUILD_DIR}/src/tools/mds-admin" /usr/local/bin/mds-admin

if [[ "${SKIP_SERVICE:-0}" -eq 1 ]]; then
  exit 0
fi

sudo systemctl stop pnfs-mds
sudo systemctl restart pnfs-mds
sudo systemctl status pnfs-mds --no-pager
