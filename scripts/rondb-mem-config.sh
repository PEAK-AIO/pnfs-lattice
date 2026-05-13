#!/usr/bin/env bash
#
# rondb-mem-config.sh - Generate a RonDB memory config sized to a fraction
# of system RAM.
#
# Use this to keep RonDB from grabbing the whole box. By default it allocates
# 50% of system memory to RonDB and leaves the rest for the OS, page cache,
# and any co-located processes (for example pnfs-mds on an MDS node).
#
# Emits a config.ini snippet (the "[ndbd default]" memory parameters) suitable
# to drop into the file consumed by ndb_mgmd. After updating config.ini on the
# management node, restart ndb_mgmd and then do a rolling restart of the data
# nodes (see the "Apply" comment at the bottom of the generated snippet).
#
# Background:
#   RonDB 24.x defaults to AutomaticMemoryConfig=1, which sizes DataMemory and
#   DiskPageBufferMemory to fill almost all available system memory. On a host
#   that is also running other services (pnfs-mds, the OS page cache, etc.)
#   this leaves no room for anything else, so we cap RonDB to a configurable
#   fraction of system RAM.
#
# Examples:
#   ./rondb-mem-config.sh                                 # 50% of local RAM
#   ./rondb-mem-config.sh -p 35                           # 35% of local RAM
#   ./rondb-mem-config.sh -H h0.lattice-mds -p 40         # remote host, 40%
#   ./rondb-mem-config.sh -m 196608 -p 50 --manual        # explicit split
#

set -euo pipefail

PROG=$(basename "$0")

PERCENT_DEFAULT=50
DATAMEM_SHARE_DEFAULT=75   # in --manual mode: % of usable budget for DataMemory

usage() {
    cat <<EOF
Usage: ${PROG} [options]

Generate a RonDB [ndbd default] memory config sized to a fraction of system
memory. The remaining memory is left for the OS, page cache, and any other
processes co-located on the host.

Options:
  -p, --percent PCT            Percent of system RAM for RonDB (default ${PERCENT_DEFAULT}).
                               Must be an integer in [5,90].
  -m, --total-mib MIB          Use this system-memory value (MiB) instead of
                               auto-detecting. Useful when generating config
                               for a host you cannot reach directly.
  -H, --ssh HOST               SSH to HOST and read its /proc/meminfo.
  -o, --output FILE            Write the snippet to FILE instead of stdout.
      --manual                 Emit explicit DataMemory + DiskPageBufferMemory
                               (with AutomaticMemoryConfig=0) instead of the
                               default AutomaticMemoryConfig=1 +
                               TotalMemoryConfig.
      --datamem-share PCT      In --manual mode, fraction of the RonDB budget
                               for DataMemory (default ${DATAMEM_SHARE_DEFAULT}).
                               The rest goes to DiskPageBufferMemory.
                               For metadata-only workloads (e.g. pnfs-mds)
                               keep this high; lower it if you use
                               disk-resident tablespaces heavily.
  -h, --help                   Show this help.

Examples:
  ${PROG}
  ${PROG} -p 35
  ${PROG} -H h0.lattice-mds.peakaio-openpnfs -p 40
  ${PROG} -m 196608 -p 50 --manual --datamem-share 80 -o ndbd-default.ini

Note on multi-node clusters:
  Run the script against each data-node host and use the lowest budget
  (the [ndbd default] section applies uniformly to all data nodes).
EOF
}

percent=${PERCENT_DEFAULT}
total_mib=
total_mib_explicit=0
ssh_host=
output=
manual=0
datamem_share=${DATAMEM_SHARE_DEFAULT}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--percent)        percent="${2:?--percent requires an argument}"; shift 2;;
        -m|--total-mib)      total_mib="${2:?--total-mib requires an argument}"
                             total_mib_explicit=1; shift 2;;
        -H|--ssh)            ssh_host="${2:?--ssh requires an argument}"; shift 2;;
        -o|--output)         output="${2:?--output requires an argument}"; shift 2;;
        --manual)            manual=1; shift;;
        --datamem-share)     datamem_share="${2:?--datamem-share requires an argument}"; shift 2;;
        -h|--help)           usage; exit 0;;
        --) shift; break;;
        -*) printf '%s: unknown option: %s\n' "${PROG}" "$1" >&2
            usage >&2; exit 2;;
        *)  printf '%s: unexpected positional argument: %s\n' "${PROG}" "$1" >&2
            usage >&2; exit 2;;
    esac
done

is_int() { [[ "$1" =~ ^[0-9]+$ ]]; }

if ! is_int "${percent}" || (( percent < 5 || percent > 90 )); then
    printf '%s: --percent must be an integer in [5,90] (got %q)\n' \
        "${PROG}" "${percent}" >&2
    exit 2
fi

if (( manual )); then
    if ! is_int "${datamem_share}" || \
       (( datamem_share < 10 || datamem_share > 95 )); then
        printf '%s: --datamem-share must be an integer in [10,95] (got %q)\n' \
            "${PROG}" "${datamem_share}" >&2
        exit 2
    fi
fi

detect_total_mib() {
    local meminfo kb
    if [[ -n "${ssh_host}" ]]; then
        meminfo=$(ssh -o BatchMode=yes -o ConnectTimeout=5 \
                      "${ssh_host}" 'cat /proc/meminfo') || {
            printf '%s: failed to read /proc/meminfo from %s\n' \
                "${PROG}" "${ssh_host}" >&2
            exit 1
        }
    else
        [[ -r /proc/meminfo ]] || {
            printf '%s: /proc/meminfo is not readable here; pass --total-mib or --ssh\n' \
                "${PROG}" >&2
            exit 1
        }
        meminfo=$(cat /proc/meminfo)
    fi
    kb=$(printf '%s\n' "${meminfo}" | awk '/^MemTotal:/ {print $2}')
    if [[ -z "${kb}" ]]; then
        printf '%s: could not parse MemTotal\n' "${PROG}" >&2
        exit 1
    fi
    printf '%s\n' $(( kb / 1024 ))
}

if [[ -z "${total_mib}" ]]; then
    total_mib=$(detect_total_mib)
fi

if ! is_int "${total_mib}" || (( total_mib < 4096 )); then
    printf '%s: total memory must be an integer >= 4096 MiB (got %q)\n' \
        "${PROG}" "${total_mib}" >&2
    exit 2
fi

# Round down to a tidy 256-MiB boundary so the emitted values are pleasant.
round_down_256() { printf '%s\n' $(( $1 / 256 * 256 )); }

rondb_mib=$(( total_mib * percent / 100 ))
rondb_mib=$(round_down_256 "${rondb_mib}")

if (( rondb_mib < 1024 )); then
    printf '%s: computed RonDB budget %d MiB is too small (need >= 1024)\n' \
        "${PROG}" "${rondb_mib}" >&2
    exit 1
fi

src_label="local /proc/meminfo"
if [[ -n "${ssh_host}" ]]; then
    src_label="${ssh_host}:/proc/meminfo"
elif (( total_mib_explicit )); then
    src_label="--total-mib"
fi

generated_at=$(date -u +%FT%TZ)

emit_auto() {
    cat <<EOF
# Generated by ${PROG} at ${generated_at}
# Source of MemTotal:    ${src_label}
# Detected total memory: ${total_mib} MiB
# RonDB allocation:      ${percent}% = ${rondb_mib} MiB
# OS / page-cache / other-process headroom: $(( total_mib - rondb_mib )) MiB
#
# Mode: AutomaticMemoryConfig (RonDB sizes DataMemory + DiskPageBufferMemory
# itself, capped by TotalMemoryConfig). Recommended unless you need a
# specific split.

[ndbd default]
AutomaticMemoryConfig=1
TotalMemoryConfig=${rondb_mib}M

# --- Apply --------------------------------------------------------------
# 1. Edit /etc/rondb/config.ini on the ndb_mgmd host and replace the existing
#    memory-related directives in [ndbd default] with the block above.
# 2. Reload the management node so it picks up the new config:
#       systemctl restart rondb-mgmd     # or:  ndb_mgmd --reload ...
# 3. Rolling-restart the data nodes, one at a time, waiting for each to
#    report "Started (RonDB-...)" before doing the next:
#       ndb_mgm -e "1 restart"
#       ndb_mgm -e "2 restart"
#    On a single-data-node cluster, "ndb_mgm -e 'all restart'" works too
#    but causes a brief unavailability.
# ------------------------------------------------------------------------
EOF
}

emit_manual() {
    # Reserve some of the budget for RonDB's fixed overheads (job buffers,
    # send buffers, replication memory, schema memory, backup buffers, and
    # the "OS overhead" RonDB allocates on its own at startup).
    local overhead_mib usable_mib datamem_mib diskbuf_mib
    overhead_mib=$(( rondb_mib * 12 / 100 ))
    usable_mib=$(( rondb_mib - overhead_mib ))
    datamem_mib=$(( usable_mib * datamem_share / 100 ))
    datamem_mib=$(round_down_256 "${datamem_mib}")
    diskbuf_mib=$(round_down_256 $(( usable_mib - datamem_mib )))

    (( datamem_mib >= 256 )) || datamem_mib=256
    (( diskbuf_mib >= 64  )) || diskbuf_mib=64

    cat <<EOF
# Generated by ${PROG} at ${generated_at}
# Source of MemTotal:    ${src_label}
# Detected total memory: ${total_mib} MiB
# RonDB allocation:      ${percent}% = ${rondb_mib} MiB
# OS / page-cache / other-process headroom: $(( total_mib - rondb_mib )) MiB
#
# Mode: explicit split (AutomaticMemoryConfig=0).
#   reserved overhead:   ${overhead_mib} MiB (job/send/repl/schema/backup, OS)
#   DataMemory:          ${datamem_mib} MiB (${datamem_share}% of usable)
#   DiskPageBufferMemory:${diskbuf_mib} MiB

[ndbd default]
AutomaticMemoryConfig=0
DataMemory=${datamem_mib}M
DiskPageBufferMemory=${diskbuf_mib}M
SharedGlobalMemory=256M
RedoBuffer=64M
TransactionMemory=128M

# --- Apply --------------------------------------------------------------
# 1. Edit /etc/rondb/config.ini on the ndb_mgmd host and replace the existing
#    memory-related directives in [ndbd default] with the block above.
# 2. Reload the management node so it picks up the new config:
#       systemctl restart rondb-mgmd     # or:  ndb_mgmd --reload ...
# 3. Rolling-restart the data nodes, one at a time, waiting for each to
#    report "Started (RonDB-...)" before doing the next:
#       ndb_mgm -e "1 restart"
#       ndb_mgm -e "2 restart"
# ------------------------------------------------------------------------
EOF
}

if (( manual )); then
    snippet=$(emit_manual)
else
    snippet=$(emit_auto)
fi

if [[ -n "${output}" ]]; then
    printf '%s\n' "${snippet}" > "${output}"
    printf '%s: wrote %s\n' "${PROG}" "${output}" >&2
else
    printf '%s\n' "${snippet}"
fi
