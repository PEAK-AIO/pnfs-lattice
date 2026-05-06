#!/usr/bin/env bash
# deps/build-ntirpc.sh -- Build and install libntirpc from source.
#
# libntirpc is the NFS-Ganesha TI-RPC library used by pnfs-mds for
# NFSv4.1 RPC handling.  Debian/Ubuntu ship it as libntirpc-dev, but
# RHEL/Rocky/Fedora do not package it.  This script builds v5.8 from
# source and installs it to /usr/local.
#
# Prerequisites (install before running):
#   Rocky/RHEL 10:
#     sudo dnf install -y gcc cmake git krb5-devel \
#         userspace-rcu-devel libnsl2-devel
#   Fedora 40+:
#     sudo dnf install -y gcc cmake git krb5-devel \
#         userspace-rcu-devel libnsl2-devel
#
# Usage:
#   sudo deps/build-ntirpc.sh
#
# After this script completes, cmake should find libntirpc automatically.

set -euo pipefail

NTIRPC_VERSION="v5.8"
NTIRPC_REPO="https://github.com/nfs-ganesha/ntirpc.git"
BUILD_DIR="/tmp/ntirpc-build-$$"

echo "==> Building libntirpc ${NTIRPC_VERSION} from source"

# Check prerequisites
for cmd in gcc cmake git; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd is required but not found. Install it first." >&2
        exit 1
    fi
done

# Clone
echo "==> Cloning ${NTIRPC_REPO} (tag ${NTIRPC_VERSION})"
git clone --depth 1 -b "${NTIRPC_VERSION}" "${NTIRPC_REPO}" "${BUILD_DIR}"

# Build
echo "==> Configuring"
cmake -S "${BUILD_DIR}" -B "${BUILD_DIR}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DUSE_GSS=ON \
    -DTIRPC_EPOLL=ON

echo "==> Compiling ($(nproc) jobs)"
cmake --build "${BUILD_DIR}/build" -j"$(nproc)"

# Install
echo "==> Installing to /usr/local"
cmake --install "${BUILD_DIR}/build"

# Update ldconfig
echo "==> Updating ldconfig"
if [ -d /etc/ld.so.conf.d ]; then
    echo "/usr/local/lib64" > /etc/ld.so.conf.d/ntirpc.conf
    echo "/usr/local/lib"  >> /etc/ld.so.conf.d/ntirpc.conf
fi
ldconfig

# Cleanup
rm -rf "${BUILD_DIR}"

echo "==> libntirpc ${NTIRPC_VERSION} installed successfully"
echo "    Library:  $(find /usr/local/lib64 /usr/local/lib -name 'libntirpc.so*' 2>/dev/null | head -1)"
echo "    Headers:  /usr/local/include/ntirpc/"
