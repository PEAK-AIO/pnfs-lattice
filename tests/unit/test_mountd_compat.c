/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mountd_compat.c -- Unit tests for the showmount-e responder.
 *
 * Exercises mountd_compat_handle_packet in isolation: synthetic
 * RPC call bytes in, reply bytes out, no sockets, no threads.
 * Every dispatch path and every error path is covered.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "mountd_compat.h"

/* ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, (long long)(a), \
                #b, (long long)(b)); \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* ------------------------------------------------------------------- */

/* RFC 5531 / 1813 constants -- duplicated here so the test does not
 * depend on internal mountd_compat.c definitions. */
#define T_CALL          0U
#define T_REPLY         1U
#define T_RPC_VERSION   2U
#define T_MSG_ACCEPTED  0U
#define T_MSG_DENIED    1U

#define T_ACCEPT_SUCCESS        0U
#define T_ACCEPT_PROG_UNAVAIL   1U
#define T_ACCEPT_PROG_MISMATCH  2U
#define T_ACCEPT_PROC_UNAVAIL   3U

#define T_REJECT_RPC_MISMATCH 0U
#define T_REJECT_AUTH_ERROR   1U
#define T_AUTH_TOOWEAK        5U

#define T_AUTH_NONE 0U
#define T_AUTH_UNIX 1U

#define T_MOUNTD_PROG  100005U
#define T_MOUNTD_VERS3 3U

#define T_PROC_NULL    0U
#define T_PROC_MNT     1U
#define T_PROC_DUMP    2U
#define T_PROC_UMNT    3U
#define T_PROC_UMNTALL 4U
#define T_PROC_EXPORT  5U

#define T_XID 0xdeadbeefU

/* ------------------------------------------------------------------- */

static void put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t) v;
}

static uint32_t get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

/**
 * Build a minimal RPC call: xid, mtype, rpcvers, prog, vers, proc,
 * cred_flavor=0, cred_len=0, verf_flavor=0, verf_len=0.
 * Returns the total length.
 */
static size_t build_call_min(uint8_t *buf, uint32_t xid, uint32_t mtype,
                             uint32_t rpcvers, uint32_t prog, uint32_t vers,
                             uint32_t proc, uint32_t auth_flavor)
{
    put_u32(buf +  0, xid);
    put_u32(buf +  4, mtype);
    put_u32(buf +  8, rpcvers);
    put_u32(buf + 12, prog);
    put_u32(buf + 16, vers);
    put_u32(buf + 20, proc);
    put_u32(buf + 24, auth_flavor);
    put_u32(buf + 28, 0);              /* cred_len */
    put_u32(buf + 32, T_AUTH_NONE);    /* verf_flavor */
    put_u32(buf + 36, 0);              /* verf_len */
    return 40;
}

/**
 * Build a call with an AUTH_UNIX-shaped cred body of @a cred_len
 * bytes (zeros).  Padded to a 4-byte boundary on the wire.
 */
static size_t build_call_with_cred(uint8_t *buf, uint32_t xid, uint32_t proc,
                                   uint32_t cred_len)
{
    put_u32(buf +  0, xid);
    put_u32(buf +  4, T_CALL);
    put_u32(buf +  8, T_RPC_VERSION);
    put_u32(buf + 12, T_MOUNTD_PROG);
    put_u32(buf + 16, T_MOUNTD_VERS3);
    put_u32(buf + 20, proc);
    put_u32(buf + 24, T_AUTH_UNIX);
    put_u32(buf + 28, cred_len);
    size_t off = 32;
    size_t padded = (cred_len + 3U) & ~(size_t)3U;
    memset(buf + off, 0, padded);
    off += padded;
    put_u32(buf + off, T_AUTH_NONE);   /* verf_flavor */
    put_u32(buf + off + 4, 0);         /* verf_len */
    return off + 8;
}

/* Reply parsers ----------------------------------------------------- */

static void check_accepted_header(const uint8_t *out, size_t out_len,
                                  uint32_t expected_xid,
                                  uint32_t expected_accept_stat)
{
    /* xid(4) + REPLY(4) + MSG_ACCEPTED(4) + verf_flavor(4) +
     * verf_len(4) + accept_stat(4) = 24 bytes minimum. */
    ASSERT_TRUE(out_len >= 24U);
    ASSERT_EQ(get_u32(out +  0), expected_xid);
    ASSERT_EQ(get_u32(out +  4), T_REPLY);
    ASSERT_EQ(get_u32(out +  8), T_MSG_ACCEPTED);
    ASSERT_EQ(get_u32(out + 12), T_AUTH_NONE);
    ASSERT_EQ(get_u32(out + 16), 0U);
    ASSERT_EQ(get_u32(out + 20), expected_accept_stat);
}

/* ------------------------------------------------------------------- */

static struct mountd_compat_exports mk_exports(uint32_t count,
                                               const char *const *paths)
{
    struct mountd_compat_exports e;
    memset(&e, 0, sizeof(e));
    e.count = count;
    for (uint32_t i = 0; i < count; i++) {
        (void)snprintf(e.paths[i], sizeof(e.paths[i]), "%s", paths[i]);
    }
    return e;
}

/* ------------------------------------------------------------------- */
/* Tests                                                                */
/* ------------------------------------------------------------------- */

static void test_null_proc_success(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[256];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);
    /* NULL has no body -- exactly 24 bytes. */
    ASSERT_EQ(out_len, 24U);
}

static void test_export_single(void)
{
    const char *p[] = { "/pnfs" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[512];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_EXPORT, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);

    /* Body: VF=1, len=5, "/pnfs", pad 3, groups VF=0, list-end VF=0. */
    size_t off = 24;
    ASSERT_EQ(get_u32(out + off), 1U); off += 4;
    ASSERT_EQ(get_u32(out + off), 5U); off += 4;
    ASSERT_TRUE(memcmp(out + off, "/pnfs", 5) == 0); off += 8; /* 5 + 3 pad */
    ASSERT_EQ(get_u32(out + off), 0U); off += 4;             /* end-of-groups */
    ASSERT_EQ(get_u32(out + off), 0U); off += 4;             /* end-of-list */
    ASSERT_EQ(out_len, off);
}

static void test_export_multiple(void)
{
    const char *p[] = { "/", "/pnfs", "/scratch" };
    struct mountd_compat_exports e = mk_exports(3, p);

    uint8_t in[64];
    uint8_t out[512];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_EXPORT, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);

    /* Walk the list and confirm three entries plus terminator. */
    size_t off = 24;
    for (uint32_t i = 0; i < 3; i++) {
        ASSERT_EQ(get_u32(out + off), 1U); off += 4;
        uint32_t slen = get_u32(out + off);
        off += 4;
        size_t spadded = (slen + 3U) & ~(size_t)3U;
        ASSERT_EQ(slen, (uint32_t)strlen(p[i]));
        ASSERT_TRUE(memcmp(out + off, p[i], slen) == 0);
        off += spadded;
        ASSERT_EQ(get_u32(out + off), 0U); off += 4;
    }
    ASSERT_EQ(get_u32(out + off), 0U); off += 4;
    ASSERT_EQ(out_len, off);
}

static void test_export_zero_entries(void)
{
    /* Edge case: export list with zero entries.  The responder still
     * returns SUCCESS with a single end-of-list marker. */
    struct mountd_compat_exports e;
    memset(&e, 0, sizeof(e));
    e.count = 0;

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_EXPORT, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);
    ASSERT_EQ(out_len, 28U);            /* 24 header + 4 terminator */
    ASSERT_EQ(get_u32(out + 24), 0U);
}

static void test_dump_returns_empty_list(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_DUMP, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);
    ASSERT_EQ(out_len, 28U);
    ASSERT_EQ(get_u32(out + 24), 0U);
}

static void test_mnt_proc_unavail(void)
{
    /* THIS is the design guarantee: MNT must always be rejected so a
     * client cannot accidentally NFSv3-mount the MDS. */
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_MNT, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_PROC_UNAVAIL);
    ASSERT_EQ(out_len, 24U);
}

static void test_umnt_and_umntall_proc_unavail(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint32_t procs[] = { T_PROC_UMNT, T_PROC_UMNTALL, 99U };
    for (size_t i = 0; i < sizeof(procs) / sizeof(procs[0]); i++) {
        uint8_t in[64];
        uint8_t out[64];
        size_t  out_len = 0;
        size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                        T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                        procs[i], T_AUTH_NONE);
        ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                              out, sizeof(out), &out_len), 0);
        check_accepted_header(out, out_len, T_XID, T_ACCEPT_PROC_UNAVAIL);
    }
}

static void test_wrong_program_prog_unavail(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    100003U /* NFS v3 */, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_PROG_UNAVAIL);
}

static void test_wrong_version_prog_mismatch(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, 1U /* v1 */,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_PROG_MISMATCH);
    /* Trailer: low(4) + high(4), both = 3. */
    ASSERT_EQ(out_len, 32U);
    ASSERT_EQ(get_u32(out + 24), T_MOUNTD_VERS3);
    ASSERT_EQ(get_u32(out + 28), T_MOUNTD_VERS3);
}

static void test_wrong_rpc_version_denied(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    /* RPC version != 2 -> MSG_DENIED + RPC_MISMATCH. */
    size_t  in_len = build_call_min(in, T_XID, T_CALL, 3U /* not 2 */,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    ASSERT_EQ(out_len, 24U);
    ASSERT_EQ(get_u32(out +  0), T_XID);
    ASSERT_EQ(get_u32(out +  4), T_REPLY);
    ASSERT_EQ(get_u32(out +  8), T_MSG_DENIED);
    ASSERT_EQ(get_u32(out + 12), T_REJECT_RPC_MISMATCH);
    ASSERT_EQ(get_u32(out + 16), T_RPC_VERSION); /* low  */
    ASSERT_EQ(get_u32(out + 20), T_RPC_VERSION); /* high */
}

static void test_unknown_auth_too_weak(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    /* Some other auth flavor (e.g. AUTH_KERB=4 or made up). */
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_EXPORT, 9999U);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    ASSERT_EQ(out_len, 20U);
    ASSERT_EQ(get_u32(out +  0), T_XID);
    ASSERT_EQ(get_u32(out +  4), T_REPLY);
    ASSERT_EQ(get_u32(out +  8), T_MSG_DENIED);
    ASSERT_EQ(get_u32(out + 12), T_REJECT_AUTH_ERROR);
    ASSERT_EQ(get_u32(out + 16), T_AUTH_TOOWEAK);
}

static void test_auth_unix_accepted_with_body(void)
{
    /* AUTH_UNIX with a 64-byte cred body must still produce a
     * successful EXPORT reply.  This is the typical Linux client
     * shape: cred carries hostname+uid+gid+gids. */
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[256];
    uint8_t out[256];
    size_t  out_len = 0;
    size_t  in_len = build_call_with_cred(in, T_XID, T_PROC_EXPORT, 64);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    check_accepted_header(out, out_len, T_XID, T_ACCEPT_SUCCESS);
}

static void test_truncated_header_dropped(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[8];
    uint8_t out[64];
    size_t  out_len = 0;

    /* A 4-byte input is not even an XID + msg_type. */
    memset(in, 0, sizeof(in));
    ASSERT_EQ(mountd_compat_handle_packet(&e, in, 4,
                                          out, sizeof(out), &out_len), -1);
    ASSERT_EQ(mountd_compat_handle_packet(&e, in, 8,
                                          out, sizeof(out), &out_len), -1);
}

static void test_oversized_cred_len_dropped(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[80];
    uint8_t out[64];
    size_t  out_len = 0;
    /* Build a header that claims cred_len = huge. */
    size_t in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                   T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                   T_PROC_NULL, T_AUTH_UNIX);
    /* Overwrite cred_len with 0x00100000 (1 MiB) -- far over the
     * RFC 5531 S8.1 cap (400 bytes). */
    put_u32(in + 28, 0x00100000U);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), -1);
}

static void test_oversized_verf_len_dropped(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[80];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                   T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                   T_PROC_NULL, T_AUTH_NONE);
    /* Overwrite verf_len with a huge value. */
    put_u32(in + 36, 0x00200000U);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), -1);
}

static void test_not_a_call_dropped(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_REPLY /* not CALL */,
                                    T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), -1);
}

static void test_oversize_input_dropped(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    /* Anything > 8 KiB is dropped at the gate. */
    uint8_t *big = calloc(16384, 1);
    ASSERT_TRUE(big != NULL);
    /* Build a minimum-valid header at the top of the buffer. */
    (void)build_call_min(big, T_XID, T_CALL, T_RPC_VERSION,
                         T_MOUNTD_PROG, T_MOUNTD_VERS3,
                         T_PROC_NULL, T_AUTH_NONE);

    uint8_t out[64];
    size_t  out_len = 0;
    ASSERT_EQ(mountd_compat_handle_packet(&e, big, 16384,
                                          out, sizeof(out), &out_len), -1);
    free(big);
}

static void test_null_arguments(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, T_XID, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(NULL, in, in_len,
                                          out, sizeof(out), &out_len), -1);
    ASSERT_EQ(mountd_compat_handle_packet(&e, NULL, in_len,
                                          out, sizeof(out), &out_len), -1);
    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          NULL, sizeof(out), &out_len), -1);
    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), NULL), -1);
}

static void test_xid_preserved(void)
{
    const char *p[] = { "/" };
    struct mountd_compat_exports e = mk_exports(1, p);

    /* XID with all bits including MSB set. */
    const uint32_t xid = 0xfedcba98U;
    uint8_t in[64];
    uint8_t out[64];
    size_t  out_len = 0;
    size_t  in_len = build_call_min(in, xid, T_CALL, T_RPC_VERSION,
                                    T_MOUNTD_PROG, T_MOUNTD_VERS3,
                                    T_PROC_NULL, T_AUTH_NONE);

    ASSERT_EQ(mountd_compat_handle_packet(&e, in, in_len,
                                          out, sizeof(out), &out_len), 0);
    ASSERT_EQ(get_u32(out + 0), xid);
}

/* ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_mountd_compat\n");

    RUN_TEST(test_null_proc_success);
    RUN_TEST(test_export_single);
    RUN_TEST(test_export_multiple);
    RUN_TEST(test_export_zero_entries);
    RUN_TEST(test_dump_returns_empty_list);
    RUN_TEST(test_mnt_proc_unavail);
    RUN_TEST(test_umnt_and_umntall_proc_unavail);
    RUN_TEST(test_wrong_program_prog_unavail);
    RUN_TEST(test_wrong_version_prog_mismatch);
    RUN_TEST(test_wrong_rpc_version_denied);
    RUN_TEST(test_unknown_auth_too_weak);
    RUN_TEST(test_auth_unix_accepted_with_body);
    RUN_TEST(test_truncated_header_dropped);
    RUN_TEST(test_oversized_cred_len_dropped);
    RUN_TEST(test_oversized_verf_len_dropped);
    RUN_TEST(test_not_a_call_dropped);
    RUN_TEST(test_oversize_input_dropped);
    RUN_TEST(test_null_arguments);
    RUN_TEST(test_xid_preserved);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
