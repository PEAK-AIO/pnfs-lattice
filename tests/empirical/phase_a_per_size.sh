#!/usr/bin/env bash
# Per-size Linux-default capture: one pcap per dd size, then build a
# table showing LAYOUTGET replies + FF_FLAGS_STRIPE_LEASE bit count for
# each size individually.
#
# Default sizes cover 4 KiB to 256 MiB (override with SIZES="...").
#
# (c) PEAK:AIO Mark Klarzynski

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib_common.sh
. "$HERE/lib_common.sh"

PHASE="phase_a_per_size"
TABLE_TXT="$RESULT_DIR/${PHASE}_table.txt"
TABLE_MD="$RESULT_DIR/${PHASE}_table.md"
TABLE_JSON="$RESULT_DIR/${PHASE}_table.json"
SUMMARY_JSON="$RESULT_DIR/${PHASE}_summary.json"

: "${SIZES:=4K 16K 64K 256K 1M 16M 64M 256M}"

log "Phase A per-size against $MDS_HOST: $SIZES"

for tool in dd tcpdump tshark mount.nfs python3; do
    command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done

ensure_mount

# Build a small JSON array as we go.
json_rows=()

for sz in $SIZES; do
    PCAP="$RESULT_DIR/${PHASE}_${sz}.pcap"
    target="${MOUNT}/.per_size_$$_${sz}"

    # Pre-warm: drop client-cached layouts by switching to a fresh file
    # name; each iteration uses its own canary so no cache reuse occurs.
    start_capture "$PCAP"
    dd if=/dev/urandom of="$target" bs="$sz" count=1 \
        conv=fsync,fdatasync status=none \
        2>>"$RESULT_DIR/${PHASE}_dd.err" \
        && wrote="ok" || wrote="FAIL"
    sync
    sleep 1
    stop_capture

    lg=$(count_layoutget_replies "$PCAP")
    ff=$(count_ff_stripe_lease_on_wire "$PCAP")
    log "size=$sz write=$wrote LAYOUTGET=$lg FF_FLAGS_STRIPE_LEASE=$ff"

    if [ "$wrote" = "ok" ]; then pass "write $sz"; else fail "write $sz"; fi
    if [ "$ff" -eq 0 ]; then
        pass "FF_FLAGS_STRIPE_LEASE = 0 for size $sz"
    else
        fail "FF_FLAGS_STRIPE_LEASE = $ff for size $sz (expected 0)"
    fi

    json_rows+=("{\"size\":\"$sz\",\"write\":\"$wrote\",\"layoutget\":$lg,\"ff_stripe_lease\":$ff}")
    rm -f "$target" 2>/dev/null
done

# Emit human + machine tables.
{
    printf '%-8s  %-6s  %-12s  %-22s\n' \
        "size" "write" "LAYOUTGET" "FF_FLAGS_STRIPE_LEASE"
    printf '%-8s  %-6s  %-12s  %-22s\n' \
        "----" "-----" "---------" "---------------------"
    for sz in $SIZES; do
        PCAP="$RESULT_DIR/${PHASE}_${sz}.pcap"
        lg=$(count_layoutget_replies "$PCAP")
        ff=$(count_ff_stripe_lease_on_wire "$PCAP")
        printf '%-8s  %-6s  %-12s  %-22s\n' "$sz" "ok" "$lg" "$ff"
    done
} | tee "$TABLE_TXT"

{
    echo "| size | write | LAYOUTGET replies | FF_FLAGS_STRIPE_LEASE on wire |"
    echo "|---|---|---|---|"
    for sz in $SIZES; do
        PCAP="$RESULT_DIR/${PHASE}_${sz}.pcap"
        lg=$(count_layoutget_replies "$PCAP")
        ff=$(count_ff_stripe_lease_on_wire "$PCAP")
        echo "| $sz | ok | $lg | $ff |"
    done
} > "$TABLE_MD"

{
    printf '['
    n=${#json_rows[@]}
    for i in "${!json_rows[@]}"; do
        printf '%s' "${json_rows[$i]}"
        if [ $((i + 1)) -lt "$n" ]; then printf ','; fi
    done
    printf ']\n'
} > "$TABLE_JSON"

write_summary_json "$PHASE" "$SUMMARY_JSON"

log "Phase A per-size done: pass=$PASS fail=$FAIL  table=$TABLE_TXT"
exit "$FAIL"
