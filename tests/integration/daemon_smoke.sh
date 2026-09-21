#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# daemon_smoke.sh -- unprivileged end-to-end smoke of pnfs-mds on the
# in-memory catalogue backend (default) or on FoundationDB.
#
# Usage: daemon_smoke.sh <build-dir>
#        DAEMON_SMOKE_BACKEND=fdb [FDB_CLUSTER_FILE=...] daemon_smoke.sh <build-dir>
#
# Writes a temporary mds.conf selecting `catalogue_backend = memdb` (or
# `fdb` with the cluster file and a private, run-unique fdb_key_prefix
# that is cleared again at exit), an unprivileged NFS port and an
# unprivileged cluster-transport port, starts <build-dir>/src/mds/pnfs-mds
# in the background, waits (bounded) for the NFS port to accept
# connections and then probes it:
#
#   * with pynfs when PYNFS_DIR/nfs4.1/testserver.py runs (default:
#     a pynfs checkout next to this repository) -- EXCHANGE_ID,
#     CREATE_SESSION, SEQUENCE, root LOOKUP/LOOKUPP cases, all
#     self-contained, no test tree needed -- or
#   * with a raw ONC-RPC NULL call to the NFS program (record-marked
#     TCP, AUTH_NONE) when pynfs is not available.
#
# Exit codes:
#   0   daemon started on the selected backend and answered the probe
#   77  skipped: running as root, no daemon binary, the daemon refused
#       the selected `catalogue_backend` (binary without that backend /
#       factory), or DAEMON_SMOKE_BACKEND=fdb without a readable cluster
#       file -- registered as SKIP_RETURN_CODE in ctest
#   1   failure (daemon died for another reason, port never came up,
#       probe failed); the daemon log tail is printed
#
# Side effects: creates and removes one temporary directory under
# ${TMPDIR:-/tmp}; starts one pnfs-mds process and kills it on exit; on
# fdb, writes under its private key prefix and clears that prefix at
# exit (fdbcli clearrange, best effort).  Never touches port 2049 or
# any other privileged port.

set -euo pipefail

SKIP=77
readonly SKIP

usage() {
    echo "usage: $0 <build-dir>" >&2
    exit 2
}

skip() {
    echo "SKIP: $*"
    exit "${SKIP}"
}

fail() {
    echo "FAIL: $*" >&2
    if [[ -n "${DAEMON_OUT:-}" && -f "${DAEMON_OUT}" ]]; then
        echo "--- daemon stdout/stderr (tail) ---" >&2
        tail -n 40 "${DAEMON_OUT}" >&2 || true
    fi
    if [[ -n "${DAEMON_LOG:-}" && -f "${DAEMON_LOG}" ]]; then
        echo "--- daemon log (tail) ---" >&2
        tail -n 40 "${DAEMON_LOG}" >&2 || true
    fi
    exit 1
}

[[ $# -eq 1 ]] || usage
BUILD_DIR="$1"
[[ -d "${BUILD_DIR}" ]] || fail "build dir '${BUILD_DIR}' does not exist"
BUILD_DIR="$(cd "${BUILD_DIR}" && pwd)"

if [[ "$(id -u)" -eq 0 ]]; then
    skip "refusing to run as root (this smoke proves unprivileged operation)"
fi

MDS_BIN="${BUILD_DIR}/src/mds/pnfs-mds"
[[ -x "${MDS_BIN}" ]] || skip "daemon binary not found at ${MDS_BIN}"

# --- backend selection (DAEMON_SMOKE_BACKEND=memdb|fdb) -------------------

SMOKE_BACKEND="${DAEMON_SMOKE_BACKEND:-memdb}"
SMOKE_FDB_PREFIX=""
SMOKE_FDB_CLUSTER=""
case "${SMOKE_BACKEND}" in
    memdb) ;;
    fdb)
        SMOKE_FDB_CLUSTER="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
        [[ -r "${SMOKE_FDB_CLUSTER}" ]] || \
            skip "DAEMON_SMOKE_BACKEND=fdb but no readable cluster file at ${SMOKE_FDB_CLUSTER}"
        # Private keyspace for this run; ends in a fixed letter so the
        # clearrange end key is the same prefix with that letter bumped.
        SMOKE_FDB_PREFIX="smoke-$$-a"
        ;;
    *)
        echo "unknown DAEMON_SMOKE_BACKEND '${SMOKE_BACKEND}' (memdb|fdb)" >&2
        exit 2
        ;;
esac

render_backend_config() {
    if [[ "${SMOKE_BACKEND}" == "fdb" ]]; then
        printf 'catalogue_backend = fdb\n'
        printf 'fdb_cluster_file = %s\n' "${SMOKE_FDB_CLUSTER}"
        printf 'fdb_key_prefix = %s\n' "${SMOKE_FDB_PREFIX}"
    else
        printf 'catalogue_backend = memdb\n'
    fi
}

# Best effort: drop everything this run wrote under its prefix.
smoke_fdb_cleanup() {
    [[ "${SMOKE_BACKEND}" == "fdb" ]] || return 0
    command -v fdbcli > /dev/null 2>&1 || return 0
    timeout 30 fdbcli -C "${SMOKE_FDB_CLUSTER}" --timeout 20 \
        --exec "writemode on; clearrange ${SMOKE_FDB_PREFIX} ${SMOKE_FDB_PREFIX%a}b" \
        > /dev/null 2>&1 || true
}

# pynfs checkout: PYNFS_DIR, else a sibling of this repository.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYNFS_DIR="${PYNFS_DIR:-${REPO_ROOT}/../pynfs}"
PYNFS_TESTSERVER="${PYNFS_DIR}/nfs4.1/testserver.py"

# --- temporary state -------------------------------------------------------

WORK="$(mktemp -d "${TMPDIR:-/tmp}/pnfs-mds-smoke.XXXXXX")"
DAEMON_OUT="${WORK}/daemon.out"
DAEMON_LOG="${WORK}/mds.log"
CONF="${WORK}/mds.conf"
DAEMON_PID=""

# Invoked through the EXIT trap only.
# shellcheck disable=SC2317
cleanup() {
    if [[ -n "${DAEMON_PID}" ]] && kill -0 "${DAEMON_PID}" 2>/dev/null; then
        kill -TERM "${DAEMON_PID}" 2>/dev/null || true
        # Bounded wait for an orderly shutdown, then force.
        for _ in $(seq 1 50); do
            if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        kill -KILL "${DAEMON_PID}" 2>/dev/null || true
        wait "${DAEMON_PID}" 2>/dev/null || true
    fi
    smoke_fdb_cleanup
    rm -rf "${WORK}"
}
trap cleanup EXIT

# --- port selection --------------------------------------------------------

# True when nothing accepts connections on 127.0.0.1:<port>.
port_free() {
    local port="$1"
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
        return 1
    fi
    return 0
}

# Both port pools sit below the kernel's ephemeral (outbound) range,
# net.ipv4.ip_local_port_range, default 32768-60999: a listener bound
# inside that range collides with whatever loopback client connection
# happens to own the port, and port_free() is a connect probe that
# sees listeners only.  Unprivileged by construction (>= 10000).
NFS_PORT_BASE=20000
NFS_PORT_SPAN=12000        # 20000..31999
GRPC_PORT_BASE=10000
GRPC_PORT_SPAN=10000       # 10000..19999
EPHEMERAL_LOW_DEFAULT=32768

ephemeral_floor() {
    local low=""
    if [[ -r /proc/sys/net/ipv4/ip_local_port_range ]]; then
        read -r low _ < /proc/sys/net/ipv4/ip_local_port_range || true
    fi
    if [[ "${low}" =~ ^[0-9]+$ ]]; then
        echo "${low}"
    else
        echo "${EPHEMERAL_LOW_DEFAULT}"
    fi
}

EPHEMERAL_LOW="$(ephemeral_floor)"
if (( NFS_PORT_BASE + NFS_PORT_SPAN > EPHEMERAL_LOW )); then
    echo "daemon_smoke: warning: ip_local_port_range starts at ${EPHEMERAL_LOW};" \
         "the smoke port pools overlap the ephemeral range"
fi

# Deterministic per pid, re-rolled while occupied; <roll> shifts the
# whole sequence so a retried start does not draw the same ports.
pick_port() {
    local base="$1"
    local span="$2"
    local roll="$3"
    local port
    local attempt
    for attempt in $(seq 0 19); do
        port=$(( base + (($$ + roll * 1000 + attempt * 7) % span) ))
        if port_free "${port}"; then
            echo "${port}"
            return 0
        fi
    done
    return 1
}

# --- configuration ---------------------------------------------------------

write_config() {
    cat > "${CONF}" <<EOF
# generated by tests/integration/daemon_smoke.sh
mds_id = 1
hostname = 127.0.0.1
nfs_port = ${NFS_PORT}
grpc_port = ${GRPC_PORT}
cluster_bind_addr = 127.0.0.1
cluster_size = 1
$(render_backend_config)
worker_threads = 2
ds_count = 0
metrics_http_port = 0
mountd_compat_enabled = false
log_file = ${DAEMON_LOG}
log_level = info
EOF
}

# --- start the daemon and wait for its listener (bounded) ------------------

WAIT_SECONDS=30

# The port probe cannot see a live outbound connection that owns the
# port, and the daemon's bind() runs after the probe anyway; both
# windows are small with the pools above, but not closed.  A daemon that
# exits before listening therefore gets one more start on freshly rolled
# ports; every attempt is logged.
START_ATTEMPTS=2

# Start the daemon on the ports for <roll> and wait for the NFS
# listener.  Returns 0 when it is up (DAEMON_PID set) and 1 when the
# daemon exited first (DAEMON_PID cleared, DAEMON_RC its status); the
# backend refusal and a listener that never comes up end the run here.
start_daemon() {
    local roll="$1"
    local deadline
    local refusal

    NFS_PORT="$(pick_port "${NFS_PORT_BASE}" "${NFS_PORT_SPAN}" "${roll}")" \
        || fail "no free NFS port found"
    GRPC_PORT="$(pick_port "${GRPC_PORT_BASE}" "${GRPC_PORT_SPAN}" "${roll}")" \
        || fail "no free cluster-transport port found"
    if [[ "${NFS_PORT}" -eq 2049 || "${GRPC_PORT}" -eq 2049 ]]; then
        fail "port selection produced 2049"
    fi
    write_config
    DAEMON_OUT="${WORK}/daemon.${roll}.out"

    echo "daemon_smoke: starting ${MDS_BIN} (backend=${SMOKE_BACKEND}" \
         "nfs_port=${NFS_PORT} grpc_port=${GRPC_PORT}" \
         "attempt=$(( roll + 1 ))/${START_ATTEMPTS})"
    "${MDS_BIN}" "${CONF}" > "${DAEMON_OUT}" 2>&1 &
    DAEMON_PID=$!

    deadline=$(( SECONDS + WAIT_SECONDS ))
    while [[ ${SECONDS} -lt ${deadline} ]]; do
        if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
            break
        fi
        if ! port_free "${NFS_PORT}"; then
            return 0
        fi
        sleep 0.2
    done

    if kill -0 "${DAEMON_PID}" 2>/dev/null; then
        fail "daemon did not listen on 127.0.0.1:${NFS_PORT} within ${WAIT_SECONDS}s"
    fi
    DAEMON_RC=0
    wait "${DAEMON_PID}" || DAEMON_RC=$?
    DAEMON_PID=""
    # The one expected refusal: this binary cannot select the backend
    # from mds.conf (config parser or factory without that backend).
    # The log file may not exist yet, hence the tolerant pipeline.
    refusal="$(cat "${DAEMON_OUT}" "${DAEMON_LOG}" 2>/dev/null \
               | grep "catalogue_backend" | head -n 3 || true)"
    if [[ -n "${refusal}" ]]; then
        echo "daemon refused catalogue_backend = ${SMOKE_BACKEND} (exit ${DAEMON_RC}):"
        echo "${refusal}"
        skip "${SMOKE_BACKEND} backend is not selectable in this build"
    fi
    echo "daemon_smoke: daemon exited with status ${DAEMON_RC} before listening" \
         "(nfs_port=${NFS_PORT} grpc_port=${GRPC_PORT})"
    return 1
}

DAEMON_RC=0
listening=0
for roll in $(seq 0 $(( START_ATTEMPTS - 1 ))); do
    if start_daemon "${roll}"; then
        listening=1
        break
    fi
    echo "--- daemon stdout/stderr of attempt $(( roll + 1 )) (tail) ---"
    tail -n 20 "${DAEMON_OUT}" || true
    if [[ -f "${DAEMON_LOG}" ]]; then
        echo "--- daemon log of attempt $(( roll + 1 )) (tail) ---"
        tail -n 5 "${DAEMON_LOG}" || true
    fi
done
if [[ ${listening} -ne 1 ]]; then
    fail "daemon exited with status ${DAEMON_RC} before listening (${START_ATTEMPTS} attempts)"
fi
echo "daemon_smoke: listener up on 127.0.0.1:${NFS_PORT}"

# --- probe -----------------------------------------------------------------

# Raw ONC-RPC NULL call to program 100003 version 4 over record-marked
# TCP with AUTH_NONE.  Reply must be REPLY / MSG_ACCEPTED / SUCCESS for
# our xid.  Everything is big-endian XDR.
probe_rpc_null() {
    local xid_hex reply
    xid_hex="$(printf '%08x' $(( ($$ & 0x7fffffff) | 0x01000000 )))"
    if ! exec 3<>"/dev/tcp/127.0.0.1/${NFS_PORT}"; then
        echo "probe: connect failed" >&2
        return 1
    fi
    # record mark: last fragment, 40 bytes; then the call header.
    {
        printf '\x80\x00\x00\x28'
        printf '%b' "\\x${xid_hex:0:2}\\x${xid_hex:2:2}\\x${xid_hex:4:2}\\x${xid_hex:6:2}"
        printf '\x00\x00\x00\x00'   # msg_type CALL
        printf '\x00\x00\x00\x02'   # rpcvers 2
        printf '\x00\x01\x86\xa3'   # prog 100003 (NFS)
        printf '\x00\x00\x00\x04'   # vers 4
        printf '\x00\x00\x00\x00'   # proc 0 (NULL)
        printf '\x00\x00\x00\x00\x00\x00\x00\x00'   # cred AUTH_NONE, len 0
        printf '\x00\x00\x00\x00\x00\x00\x00\x00'   # verf AUTH_NONE, len 0
    } >&3
    # reply: mark(4) xid(4) msg_type(4) reply_stat(4) verf(8) accept_stat(4)
    reply="$(timeout 10 head -c 28 <&3 | od -An -tx1 -v | tr -d ' \n')"
    exec 3<&- 3>&-
    if [[ ${#reply} -ne 56 ]]; then
        echo "probe: short reply (${#reply} hex chars)" >&2
        return 1
    fi
    if [[ "${reply:8:8}" != "${xid_hex}" ]]; then
        echo "probe: xid mismatch (${reply:8:8} != ${xid_hex})" >&2
        return 1
    fi
    if [[ "${reply:16:8}" != "00000001" ]]; then
        echo "probe: not a REPLY (${reply:16:8})" >&2
        return 1
    fi
    if [[ "${reply:24:8}" != "00000000" ]]; then
        echo "probe: reply not ACCEPTED (${reply:24:8})" >&2
        return 1
    fi
    if [[ "${reply:48:8}" != "00000000" ]]; then
        echo "probe: NULL accept_stat ${reply:48:8}" >&2
        return 1
    fi
    return 0
}

# pynfs NFSv4.1: EXCHANGE_ID, CREATE_SESSION, SEQUENCE, PUTFH and
# LOOKUPP from the root.  (st_lookup.py is not registered in pynfs'
# server41tests, so no LOOK* code exists for minor version 1.)
# --noinit keeps it away from the /tmp test tree the full suite expects.
probe_pynfs() {
    local out
    out="${WORK}/pynfs.out"
    if ! (cd "${PYNFS_DIR}/nfs4.1" && \
          timeout 120 python3 "${PYNFS_TESTSERVER}" --minorversion=1 --noinit \
              "127.0.0.1:${NFS_PORT}/" EID1 CSESS1 SEQ1 PUTFH2 LKPP2 \
              > "${out}" 2>&1); then
        echo "pynfs: testserver.py exited non-zero" >&2
        tail -n 40 "${out}" >&2 || true
        return 1
    fi
    tail -n 12 "${out}"
    # "Of those: N Skipped, N Failed, N Warned, N Passed"
    local failed passed
    failed="$(sed -n 's/.*Of those: [0-9]* Skipped, \([0-9]*\) Failed.*/\1/p' "${out}" | tail -n 1)"
    passed="$(sed -n 's/.*Warned, \([0-9]*\) Passed.*/\1/p' "${out}" | tail -n 1)"
    if [[ -z "${failed}" || -z "${passed}" ]]; then
        echo "pynfs: no summary line found" >&2
        return 1
    fi
    if [[ "${failed}" -ne 0 || "${passed}" -lt 1 ]]; then
        echo "pynfs: ${failed} failed, ${passed} passed" >&2
        return 1
    fi
    return 0
}

pynfs_usable() {
    [[ -f "${PYNFS_TESTSERVER}" ]] || return 1
    command -v python3 > /dev/null 2>&1 || return 1
    (cd "${PYNFS_DIR}/nfs4.1" && python3 "${PYNFS_TESTSERVER}" --help \
        > /dev/null 2>&1) || return 1
    return 0
}

if pynfs_usable; then
    echo "daemon_smoke: probing with pynfs (${PYNFS_TESTSERVER})"
    probe_pynfs || fail "pynfs probe failed"
else
    echo "daemon_smoke: pynfs not usable; probing with a raw RPC NULL call"
    probe_rpc_null || fail "RPC NULL probe failed"
fi

if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
    fail "daemon died during the probe"
fi

echo "daemon_smoke: PASS (pnfs-mds on catalogue_backend=${SMOKE_BACKEND} answered on port ${NFS_PORT})"
exit 0
