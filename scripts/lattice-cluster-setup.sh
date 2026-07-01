#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# lattice-cluster-setup -- one-shot build + deploy + cluster bring-up for a
# RonDB-colocated Lattice (pNFS) cluster, driven from a control host.
#
# Given a list of MDS IPs and a list of DS IPs this script:
#
#   1. build      Build pnfs-mds + mds-admin once (here, or on --build-host).
#   2. genconfig  Generate config.ini / rondb.conf / mds-<n>.conf via
#                 scripts/lattice-genconfig.
#   3. deploy     Push binaries, systemd units and configs to every MDS,
#                 and the DS export mounts (/mnt/dsN) onto each MDS.
#   4. rondb-up   Start ndb_mgmd on the management MDS, then a RonDB data
#                 node (ndbmtd) on every MDS, and wait for the cluster.
#   5. mds-up     Start pnfs-mds on every MDS.
#   6. verify     Confirm cluster membership + subtree ownership through
#                 mds-admin, so referrals resolve to the right peer.
#
# Run a single phase by name, several in sequence, or "all" (default).
# Everything is idempotent: re-running on a live cluster just re-pushes
# and restarts. Nothing is ever deleted.
#
# This is a LAB orchestrator. Like scripts/pnfs-lab it supports --dry-run
# for every phase and keeps SSH/sudo assumptions explicit.
#
# ---------------------------------------------------------------------------
# Two networks
# ---------------------------------------------------------------------------
# The IPs in --mds-file / --ds-file are the addresses written INTO the
# configs (the NFS / RoCE data plane, e.g. 10.117.x.y). SSH/management may
# ride a different network (e.g. 10.114.x.y). Use --ssh-net to translate the
# leading octets of a data IP into its management IP for SSH only:
#
#     --ssh-net 10.114        # 10.117.1.52  -> ssh 10.114.1.52
#     --ssh-jump bastion      # ProxyJump host for every SSH/scp hop
#
# Without --ssh-net the data IP is used for SSH as well.
#
# Usage:
#   lattice-cluster-setup --mds-file MDS.txt --ds-file DS.txt [phase ...] [opts]
#   lattice-cluster-setup --help
#
set -euo pipefail
IFS=$'\n\t'

PROG="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# --- Defaults ---------------------------------------------------------------
mds_file=""
ds_file=""
out_dir=""                       # genconfig output; default: mktemp
ssh_user="${LATTICE_SSH_USER:-$(id -un)}"
ssh_net=""                       # e.g. 10.114  (translate data prefix for SSH)
ssh_jump="${LATTICE_SSH_JUMP:-}" # ProxyJump host (optional)
mgmd_index=1                     # 1-based MDS index that runs ndb_mgmd
build_host=""                    # where to build; default: control host (local)
conf_dir="/etc/pnfs-mds"
rondb_conf_dir="/etc/rondb"
rondb_bin="/opt/rondb/bin"
mgm_port="1186"
bin_src=""                       # dir holding built pnfs-mds + mds-admin
nfs_mount_opts="vers=4.2,proto=tcp,nconnect=16,nosharecache,hard,timeo=600,retrans=2,sec=sys"
dry_run=0
assume_yes=0
rondb_wait=120                   # seconds to wait for RonDB "Cluster up"
genconfig_extra=()               # passthrough flags for lattice-genconfig

# Populated at runtime.
MDS_IPS=()
DS_IPS=()

# --- Logging ----------------------------------------------------------------
die()  { printf '%s: error: %s\n' "${PROG}" "$*" >&2; exit 1; }
warn() { printf '%s: warning: %s\n' "${PROG}" "$*" >&2; }
info() { printf '%s: %s\n'        "${PROG}" "$*" >&2; }
step() { printf '\n=== %s ===\n'  "$*" >&2; }

usage() {
    cat <<EOF
Usage: ${PROG} --mds-file FILE --ds-file FILE [PHASE ...] [options]

Build, deploy and bring up a RonDB-colocated Lattice (pNFS) cluster.

Phases (run in this order; default is "all"):
  build       Build pnfs-mds + mds-admin (local host or --build-host).
  genconfig   Generate configs from the MDS/DS lists (lattice-genconfig).
  deploy      Push binaries, systemd units and configs to every MDS.
  rondb-up    Start ndb_mgmd (mgmt MDS) + ndbmtd (all MDS); wait for cluster.
  mds-up      Start pnfs-mds on every MDS.
  verify      Check cluster membership + subtree ownership via mds-admin.
  all         build + genconfig + deploy + rondb-up + mds-up + verify.

Required:
  --mds-file FILE     MDS data-plane IPs, one per line (# comments ok).
  --ds-file FILE      DS data-plane IPs, one per line.

SSH / addressing:
  --ssh-user USER     SSH user with passwordless sudo on every host [${ssh_user}].
  --ssh-net PREFIX    Translate the leading octets of each data IP to this
                      network for SSH only (e.g. 10.114). Default: ssh to the
                      data IP directly.
  --ssh-jump HOST     ProxyJump host for all SSH/scp hops.

Topology:
  --mgmd-index N      1-based MDS index that runs ndb_mgmd [${mgmd_index}].
  --build-host IP     Build on this host instead of locally. Data IP; gets
                      translated by --ssh-net like any other.
  --bin-src DIR       Skip building; take pnfs-mds + mds-admin from DIR.

Paths / ports (defaults shown):
  --conf-dir DIR      pnfs-mds config dir [${conf_dir}].
  --rondb-conf-dir D  ndb_mgmd config dir [${rondb_conf_dir}].
  --rondb-bin DIR     RonDB bin dir [${rondb_bin}].
  --mgm-port PORT     ndb_mgmd port [${mgm_port}].
  --rondb-wait SECS   Max wait for RonDB cluster up [${rondb_wait}].

Behaviour:
  --out DIR           genconfig output dir [a fresh mktemp dir].
  --genconfig-arg X   Extra arg passed through to lattice-genconfig (repeatable),
                      e.g. --genconfig-arg --data-memory --genconfig-arg 8G.
  -n, --dry-run       Print every remote command instead of running it.
  -y, --yes           Do not prompt before acting on the cluster.
  -h, --help          This help.

Examples:
  printf '10.117.1.57\n10.117.1.33\n10.117.1.38\n' > mds.txt
  printf '10.117.1.15\n10.117.1.43\n10.117.1.54\n' > ds.txt

  # Full bring-up, SSH over the 10.114 mgmt net via a bastion, mgmd on MDS #3:
  ${PROG} --mds-file mds.txt --ds-file ds.txt \\
      --ssh-net 10.114 --ssh-jump bastion --mgmd-index 3 -y

  # Just re-push configs and restart pnfs-mds after a config change:
  ${PROG} --mds-file mds.txt --ds-file ds.txt deploy mds-up --ssh-net 10.114
EOF
}

# --- Arg parsing ------------------------------------------------------------
PHASES=()
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --mds-file)       mds_file="${2:?}"; shift 2;;
            --ds-file)        ds_file="${2:?}"; shift 2;;
            --out)            out_dir="${2:?}"; shift 2;;
            --ssh-user)       ssh_user="${2:?}"; shift 2;;
            --ssh-net)        ssh_net="${2:?}"; shift 2;;
            --ssh-jump)       ssh_jump="${2:?}"; shift 2;;
            --mgmd-index)     mgmd_index="${2:?}"; shift 2;;
            --build-host)     build_host="${2:?}"; shift 2;;
            --bin-src)        bin_src="${2:?}"; shift 2;;
            --conf-dir)       conf_dir="${2:?}"; shift 2;;
            --rondb-conf-dir) rondb_conf_dir="${2:?}"; shift 2;;
            --rondb-bin)      rondb_bin="${2:?}"; shift 2;;
            --mgm-port)       mgm_port="${2:?}"; shift 2;;
            --rondb-wait)     rondb_wait="${2:?}"; shift 2;;
            --genconfig-arg)  genconfig_extra+=("${2:?}"); shift 2;;
            -n|--dry-run)     dry_run=1; shift;;
            -y|--yes)         assume_yes=1; shift;;
            -h|--help)        usage; exit 0;;
            --) shift; break;;
            -*) usage >&2; die "unknown option: $1";;
            build|genconfig|deploy|rondb-up|mds-up|verify|all)
                              PHASES+=("$1"); shift;;
            *)  usage >&2; die "unexpected argument: $1";;
        esac
    done
    [[ ${#PHASES[@]} -gt 0 ]] || PHASES=(all)
}

# --- IP helpers -------------------------------------------------------------
is_ipv4() {
    local ip="$1" a b c d extra o
    IFS=. read -r a b c d extra <<<"${ip}"
    [[ -n "${a}" && -n "${b}" && -n "${c}" && -n "${d}" && -z "${extra}" ]] || return 1
    for o in "${a}" "${b}" "${c}" "${d}"; do
        [[ "${o}" =~ ^[0-9]{1,3}$ ]] && (( 10#${o} <= 255 )) || return 1
    done
}

read_list() {
    local file="$1" label="$2"; local -n out_ref="$3"
    local line ip lineno=0; local -A seen=(); out_ref=()
    [[ -f "${file}" ]] || die "${label} file not found: ${file}"
    while IFS= read -r line || [[ -n "${line}" ]]; do
        lineno=$(( lineno + 1 ))
        line="${line%%#*}"; line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "${line}" ]] && continue
        ip="${line}"
        is_ipv4 "${ip}" || die "${label} ${file}:${lineno}: not an IPv4: '${ip}'"
        [[ -z "${seen[${ip}]:-}" ]] || die "${label} ${file}: duplicate IP: ${ip}"
        seen[${ip}]=1; out_ref+=("${ip}")
    done < "${file}"
    [[ ${#out_ref[@]} -gt 0 ]] || die "${label} ${file} contains no IPs"
}

# data IP -> SSH address (apply --ssh-net prefix translation if set)
ssh_addr() {
    local data_ip="$1"
    if [[ -z "${ssh_net}" ]]; then printf '%s' "${data_ip}"; return; fi
    local a b c d; IFS=. read -r a b c d <<<"${data_ip}"
    local na nb; IFS=. read -r na nb _ <<<"${ssh_net}"
    printf '%s.%s.%s.%s' "${na}" "${nb}" "${c}" "${d}"
}

# --- SSH / scp wrappers (honour --ssh-jump and --dry-run) -------------------
ssh_base_opts=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes)

run_ssh() {            # run_ssh DATA_IP "remote shell command"
    local addr; addr="$(ssh_addr "$1")"; shift
    local -a cmd=(ssh "${ssh_base_opts[@]}")
    [[ -n "${ssh_jump}" ]] && cmd+=(-J "${ssh_jump}")
    cmd+=("${ssh_user}@${addr}" "$*")
    if [[ ${dry_run} -eq 1 ]]; then printf '[dry-run] %s\n' "${cmd[*]}" >&2; return 0; fi
    "${cmd[@]}"
}

run_scp() {            # run_scp DATA_IP LOCAL REMOTE
    local addr; addr="$(ssh_addr "$1")"; local local_path="$2" remote_path="$3"
    local -a cmd=(scp "${ssh_base_opts[@]}")
    [[ -n "${ssh_jump}" ]] && cmd+=(-J "${ssh_jump}")
    cmd+=("${local_path}" "${ssh_user}@${addr}:${remote_path}")
    if [[ ${dry_run} -eq 1 ]]; then printf '[dry-run] %s\n' "${cmd[*]}" >&2; return 0; fi
    "${cmd[@]}"
}

# scp to a root-owned dir via a staging path + sudo install (no root SSH).
push_file() {          # push_file DATA_IP LOCAL REMOTE_DEST MODE
    local ip="$1" local_path="$2" dest="$3" mode="${4:-644}"
    local stage="/tmp/${PROG}.$$.$(basename "${dest}")"
    run_scp "${ip}" "${local_path}" "${stage}"
    run_ssh "${ip}" "sudo install -D -m ${mode} ${stage} ${dest} && rm -f ${stage}"
}

confirm() {
    [[ ${assume_yes} -eq 1 || ${dry_run} -eq 1 ]] && return 0
    local reply
    printf '%s: about to act on %d MDS / %d DS. Continue? [y/N] ' \
        "${PROG}" "${#MDS_IPS[@]}" "${#DS_IPS[@]}" >&2
    read -r reply
    [[ "${reply}" =~ ^[Yy]$ ]] || die "aborted"
}

# ---------------------------------------------------------------------------
# systemd unit templates (match the lab's existing units).
# ---------------------------------------------------------------------------
emit_unit_mgmd() {     # $1 = output file
    cat > "$1" <<EOF
[Unit]
Description=RonDB management node (ndb_mgmd)
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
ExecStart=${rondb_bin}/ndb_mgmd -f ${rondb_conf_dir}/config.ini --configdir=/var/lib/rondb/mgm --reload
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
}

emit_unit_ndbmtd() {   # $1 = output file, $2 = nodeid, $3 = connectstring
    cat > "$1" <<EOF
[Unit]
Description=RonDB data node ndbmtd (NodeId=$2)
After=network-online.target rondb-mgmd.service
Wants=network-online.target

[Service]
Type=simple
# Auto --initial only on first boot (when the node fs is empty).
ExecStart=/bin/bash -c 'FS="/var/lib/rondb/data/ndb_$2_fs"; if [ -d "\$FS" ] && [ -n "\$(ls -A "\$FS" 2>/dev/null)" ]; then exec ${rondb_bin}/ndbmtd --nodaemon --ndb-connectstring=$3 --ndb-nodeid=$2; else echo "ndbmtd[NodeId=$2]: \$FS empty, starting with --initial" >&2; exec ${rondb_bin}/ndbmtd --nodaemon --ndb-connectstring=$3 --ndb-nodeid=$2 --initial; fi'
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
}

emit_unit_pnfs_mds() { # $1 = output file
    cat > "$1" <<EOF
[Unit]
Description=pnfs-mds daemon
After=network-online.target rondb-mgmd.service rondb-ndbmtd.service remote-fs.target pnfs-ds-mounts.service
Wants=network-online.target
Requires=pnfs-ds-mounts.service

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=${rondb_bin%/bin}/lib:${rondb_bin%/bin}/lib/mysql
ExecStart=/usr/local/bin/pnfs-mds ${conf_dir}/mds.conf
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
}

emit_unit_ds_mounts() { # $1 = output file, $2 = space-separated mountpoints
    local mp="$2"
    cat > "$1" <<EOF
[Unit]
Description=Mount DS NFS exports for pnfs-mds
After=network-online.target remote-fs.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c 'for m in ${mp}; do if ! mountpoint -q "\$m"; then mount "\$m"; fi; done'

[Install]
WantedBy=multi-user.target
EOF
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------
phase_build() {
    step "build"
    if [[ -n "${bin_src}" ]]; then
        info "using prebuilt binaries from ${bin_src}, skipping build"
        return 0
    fi
    if [[ -n "${build_host}" ]]; then
        info "building on ${build_host}"
        run_ssh "${build_host}" \
            "cd ${REPO_ROOT} && ./scripts/build-install-mds.sh SKIP_SERVICE=1"
        bin_src="REMOTE:${build_host}"
    else
        info "building locally via scripts/build-install-mds.sh (SKIP_INSTALL=1)"
        if [[ ${dry_run} -eq 1 ]]; then
            printf '[dry-run] SKIP_INSTALL=1 %s/scripts/build-install-mds.sh\n' "${REPO_ROOT}" >&2
        else
            SKIP_INSTALL=1 "${REPO_ROOT}/scripts/build-install-mds.sh"
        fi
        bin_src="${REPO_ROOT}/${BUILD_DIR:-build}"
    fi
}

phase_genconfig() {
    step "genconfig"
    [[ -n "${out_dir}" ]] || out_dir="$(mktemp -d -t lattice-cfg.XXXXXX)"
    info "generating configs into ${out_dir}"
    local -a gc=("${SCRIPT_DIR}/lattice-genconfig"
        --mds-file "${mds_file}" --ds-file "${ds_file}"
        --out "${out_dir}" --force --conf-dir "${conf_dir}")
    [[ ${#genconfig_extra[@]} -gt 0 ]] && gc+=("${genconfig_extra[@]}")
    if [[ ${dry_run} -eq 1 ]]; then
        printf '[dry-run] %s\n' "${gc[*]}" >&2
    else
        "${gc[@]}"
    fi
    [[ ${dry_run} -eq 1 || -f "${out_dir}/MANIFEST.txt" ]] || die "genconfig produced no MANIFEST"
}

# Resolve bin_src to a control-host directory holding pnfs-mds + mds-admin.
# If built on a remote --build-host, pull the two binaries here once.
STAGE_BIN=""
stage_binaries() {
    if [[ "${bin_src}" == REMOTE:* ]]; then
        local bh="${bin_src#REMOTE:}"
        STAGE_BIN="$(mktemp -d -t lattice-bin.XXXXXX)"
        info "fetching binaries from build host ${bh}"
        if [[ ${dry_run} -eq 1 ]]; then
            printf '[dry-run] scp %s:{pnfs-mds,mds-admin} -> %s\n' "${bh}" "${STAGE_BIN}" >&2
            return 0
        fi
        run_scp "${bh}" "${REPO_ROOT}/${BUILD_DIR:-build}/src/mds/pnfs-mds"   "${STAGE_BIN}/" 2>/dev/null \
            || run_ssh "${bh}" "cat ${REPO_ROOT}/${BUILD_DIR:-build}/src/mds/pnfs-mds"   > "${STAGE_BIN}/pnfs-mds"
        run_scp "${bh}" "${REPO_ROOT}/${BUILD_DIR:-build}/src/tools/mds-admin" "${STAGE_BIN}/" 2>/dev/null \
            || run_ssh "${bh}" "cat ${REPO_ROOT}/${BUILD_DIR:-build}/src/tools/mds-admin" > "${STAGE_BIN}/mds-admin"
    else
        # Local build tree (bin_src=.../build) or an explicit --bin-src dir.
        if [[ -f "${bin_src}/src/mds/pnfs-mds" ]]; then
            STAGE_BIN="${bin_src}/.staged"; mkdir -p "${STAGE_BIN}"
            ln -sf "${bin_src}/src/mds/pnfs-mds"    "${STAGE_BIN}/pnfs-mds"
            ln -sf "${bin_src}/src/tools/mds-admin" "${STAGE_BIN}/mds-admin"
        else
            STAGE_BIN="${bin_src}"   # flat dir already holding both binaries
        fi
    fi
    [[ ${dry_run} -eq 1 ]] && return 0
    for b in pnfs-mds mds-admin; do
        [[ -e "${STAGE_BIN}/${b}" ]] || die "binary not found: ${STAGE_BIN}/${b} (build first or fix --bin-src)"
    done
}

deploy_binaries_one() { # $1 = data IP
    local ip="$1"
    push_file "${ip}" "${STAGE_BIN}/pnfs-mds"  /usr/local/bin/pnfs-mds  755
    push_file "${ip}" "${STAGE_BIN}/mds-admin" /usr/local/bin/mds-admin 755
}

phase_deploy() {
    step "deploy"
    [[ -n "${out_dir}" && ( ${dry_run} -eq 1 || -d "${out_dir}" ) ]] \
        || die "no generated config dir; run the genconfig phase first"

    [[ -n "${bin_src}" ]] || die "no binaries: run the build phase or pass --bin-src"
    stage_binaries

    local n="${#MDS_IPS[@]}" m="${#DS_IPS[@]}"
    local mgmd_ip="${MDS_IPS[$(( mgmd_index - 1 ))]}"
    local connectstring; connectstring="$(ssh_addr "${mgmd_ip}"):${mgm_port}"

    # DS mountpoints (match ds_mount_path_fmt default /mnt/dsN).
    local mps="" i
    for (( i=0; i<m; i++ )); do mps+="/mnt/ds${i} "; done
    mps="${mps% }"

    # Build the (host-independent) unit files once into the config dir.
    local unitdir="${out_dir}/units"
    if [[ ${dry_run} -eq 0 ]]; then
        mkdir -p "${unitdir}"
        emit_unit_mgmd      "${unitdir}/rondb-mgmd.service"
        emit_unit_pnfs_mds  "${unitdir}/pnfs-mds.service"
        emit_unit_ds_mounts "${unitdir}/pnfs-ds-mounts.service" "${mps}"
    fi

    local idx
    for idx in "${!MDS_IPS[@]}"; do
        local ip="${MDS_IPS[idx]}" mds_id=$(( idx + 1 ))
        info "deploy -> MDS ${mds_id} (${ip}, ssh $(ssh_addr "${ip}"))"

        deploy_binaries_one "${ip}"

        # Configs.
        push_file "${ip}" "${out_dir}/rondb.conf"        "${conf_dir}/rondb.conf"   644
        push_file "${ip}" "${out_dir}/mds-${mds_id}.conf" "${conf_dir}/mds.conf"     644

        # RonDB data-node unit (per-host nodeid = mds_id).
        local ndbunit="${out_dir}/rondb-ndbmtd.${mds_id}.service"
        [[ ${dry_run} -eq 0 ]] && emit_unit_ndbmtd "${ndbunit}" "${mds_id}" "${connectstring}"
        push_file "${ip}" "${ndbunit}" /etc/systemd/system/rondb-ndbmtd.service 644

        # Shared units.
        push_file "${ip}" "${unitdir}/pnfs-mds.service"      /etc/systemd/system/pnfs-mds.service      644
        push_file "${ip}" "${unitdir}/pnfs-ds-mounts.service" /etc/systemd/system/pnfs-ds-mounts.service 644

        # ndb_mgmd lives on the management MDS only.
        if (( mds_id == mgmd_index )); then
            push_file "${ip}" "${out_dir}/config.ini"        "${rondb_conf_dir}/config.ini" 644
            push_file "${ip}" "${unitdir}/rondb-mgmd.service" /etc/systemd/system/rondb-mgmd.service 644
        fi

        # Ensure DS mountpoints + fstab entries exist on this MDS.
        local j
        for (( j=0; j<m; j++ )); do
            local ds_ip="${DS_IPS[j]}" mp="/mnt/ds${j}"
            run_ssh "${ip}" \
                "sudo mkdir -p ${mp}; grep -q ' ${mp} ' /etc/fstab || echo '${ds_ip}:/export/ds ${mp} nfs4 _netdev,vers=4.2,proto=tcp,sec=sys 0 0' | sudo tee -a /etc/fstab >/dev/null"
        done

        run_ssh "${ip}" "sudo systemctl daemon-reload"
    done
}

phase_rondb_up() {
    step "rondb-up"
    local mgmd_ip="${MDS_IPS[$(( mgmd_index - 1 ))]}"
    local connectstring; connectstring="$(ssh_addr "${mgmd_ip}"):${mgm_port}"

    info "start ndb_mgmd on MDS ${mgmd_index} (${mgmd_ip})"
    run_ssh "${mgmd_ip}" "sudo systemctl enable --now rondb-mgmd.service"

    local ip mds_id
    for mds_id in "${!MDS_IPS[@]}"; do
        ip="${MDS_IPS[mds_id]}"
        info "start ndbmtd (NodeId=$(( mds_id + 1 ))) on ${ip}"
        run_ssh "${ip}" "sudo systemctl enable --now rondb-ndbmtd.service"
    done

    info "waiting up to ${rondb_wait}s for RonDB cluster to come up"
    if [[ ${dry_run} -eq 1 ]]; then
        printf '[dry-run] ndb_mgm -c %s -e show (poll)\n' "${connectstring}" >&2
        return 0
    fi
    local deadline=$(( SECONDS + rondb_wait ))
    while (( SECONDS < deadline )); do
        if run_ssh "${mgmd_ip}" \
            "${rondb_bin}/ndb_mgm -c ${connectstring} -e show 2>/dev/null | grep -q '@.*(RonDB.*)'"; then
            info "RonDB cluster reports data nodes connected"
            info "creating ndb_index_stat system tables"
            run_ssh "${mgmd_ip}" "${rondb_bin}/ndb_index_stat --sys-create-if-not-exist -c ${connectstring} >/dev/null 2>&1 || true"
            run_ssh "${mgmd_ip}" "${rondb_bin}/ndb_mgm -c ${connectstring} -e show" || true
            return 0
        fi
        sleep 5
    done
    die "RonDB cluster did not come up within ${rondb_wait}s (check ndb_mgm -e show)"
}

phase_mds_up() {
    step "mds-up"
    local idx ip
    for idx in "${!MDS_IPS[@]}"; do
        ip="${MDS_IPS[idx]}"
        info "start pnfs-mds on MDS $(( idx + 1 )) (${ip})"
        run_ssh "${ip}" "sudo systemctl enable --now pnfs-ds-mounts.service || true"
        run_ssh "${ip}" "sudo systemctl enable pnfs-mds.service && sudo systemctl restart pnfs-mds.service"
    done
}

phase_verify() {
    step "verify"
    # Talk to the cluster transport from an allowlisted peer (a cluster_peer
    # of the target). We query each MDS's transport from the *next* MDS so the
    # source IP is in the target's allowlist; fall back to localhost loopback.
    local n="${#MDS_IPS[@]}" idx ok=1
    local ref_ip="${MDS_IPS[0]}"          # MDS we run mds-admin against
    local via_ip="${MDS_IPS[1]:-${MDS_IPS[0]}}"  # an allowlisted peer of ref_ip
    local ref_data="${ref_ip}"

    info "querying cluster status (mds-admin) for ${ref_ip} from peer ${via_ip}"
    if [[ ${dry_run} -eq 1 ]]; then
        printf '[dry-run] mds-admin status --mds-host %s --mds-port 50051\n' "${ref_data}" >&2
        return 0
    fi
    local out
    out="$(run_ssh "${via_ip}" \
        "sudo mds-admin status --mds-host ${ref_data} --mds-port 50051" 2>&1)" || true
    printf '%s\n' "${out}"

    # Every MDS id must appear as a member, else referrals to it return empty
    # FS_LOCATIONS and clients hang following NFS4 referrals.
    for (( idx=1; idx<=n; idx++ )); do
        if ! grep -qE "MDS[[:space:]]+${idx}\b" <<<"${out}"; then
            warn "MDS ${idx} not present in cluster membership"
            ok=0
        fi
    done
    if (( ok == 1 )); then
        info "all ${n} MDS present in membership; referrals will resolve"
    else
        warn "membership incomplete -- shard referrals to missing MDS will fail"
        return 1
    fi
}

run_phase() {
    case "$1" in
        build)     phase_build;;
        genconfig) phase_genconfig;;
        deploy)    phase_deploy;;
        rondb-up)  phase_rondb_up;;
        mds-up)    phase_mds_up;;
        verify)    phase_verify;;
        all)       phase_build; phase_genconfig; phase_deploy
                   phase_rondb_up; phase_mds_up; phase_verify;;
        *) die "unknown phase: $1";;
    esac
}

main() {
    parse_args "$@"
    [[ -n "${mds_file}" ]] || { usage >&2; die "--mds-file is required"; }
    [[ -n "${ds_file}"  ]] || { usage >&2; die "--ds-file is required"; }
    read_list "${mds_file}" "MDS" MDS_IPS
    read_list "${ds_file}"  "DS"  DS_IPS
    (( mgmd_index >= 1 && mgmd_index <= ${#MDS_IPS[@]} )) \
        || die "--mgmd-index ${mgmd_index} out of range (1..${#MDS_IPS[@]})"

    info "MDS: ${MDS_IPS[*]}"
    info "DS:  ${DS_IPS[*]}"
    info "phases: ${PHASES[*]}"
    [[ ${dry_run} -eq 1 ]] && info "DRY RUN -- no remote changes will be made"

    # Confirm before any phase that mutates the cluster.
    local p needs_confirm=0
    for p in "${PHASES[@]}"; do
        case "${p}" in deploy|rondb-up|mds-up|all) needs_confirm=1;; esac
    done
    (( needs_confirm == 1 )) && confirm

    for p in "${PHASES[@]}"; do run_phase "${p}"; done
    step "done"
}

main "$@"
