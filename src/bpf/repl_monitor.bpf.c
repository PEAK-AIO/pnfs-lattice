/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * repl_monitor.bpf.c -- eBPF programs for replication monitoring.
 *
 * Attaches to kernel TCP send/recv paths and catalogue commit to
 * measure replication latency and throughput.
 *
 * Build with: clang -O2 -target bpf -c repl_monitor.bpf.c
 */

/* TODO: when building, this needs vmlinux.h or libbpf CO-RE headers.
 * For now this is a structural placeholder. */

#if 0  /* Placeholder -- uncomment when libbpf/BTF is available */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

struct repl_latency_event {
    __u64 txn_id;
    __u64 send_ns;
    __u64 ack_ns;
    __u32 bytes;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} repl_events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1024);
    __type(key, __u64);   /* txn_id */
    __type(value, __u64); /* send timestamp */
} inflight SEC(".maps");

/* Track TCP send of replication delta */
SEC("fentry/tcp_sendmsg")
int BPF_PROG(trace_tcp_send, struct sock *sk, struct msghdr *msg,
             size_t size)
{
    /* TODO: filter by destination port (repl port)
     * Record send timestamp in inflight map keyed by txn_id
     * (parsed from first bytes of message)
     */
    return 0;
}

/* Track TCP recv of replication ACK */
SEC("fentry/tcp_recvmsg")
int BPF_PROG(trace_tcp_recv, struct sock *sk, struct msghdr *msg,
             size_t len, int flags, int *addr_len)
{
    /* TODO: filter by source port (repl port)
     * Lookup inflight entry, compute latency, emit to ring buffer
     */
    return 0;
}

char LICENSE[] SEC("license") = "Proprietary";

#endif /* placeholder */
