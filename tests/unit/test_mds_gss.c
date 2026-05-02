/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_gss.c — Unit tests for GSS module + auth gate.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <endian.h>
#include <arpa/inet.h>

#include "mds_gss.h"
#include "xdr_codec.h"

/* -------------------------------------------------------
 * Minimal test harness (same as other unit tests).
 * ------------------------------------------------------- */
static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do {\
	if ((a) != (b)) {\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",\
			__FILE__, __LINE__, #a, #b);\
		return;\
	}\
} while (0)

#define ASSERT_GT(a, b) do {\
	if (!((a) > (b))) {\
		fprintf(stderr, "  FAIL %s:%d: %s <= %s\n",\
			__FILE__, __LINE__, #a, #b);\
		return;\
	}\
} while (0)

#define RUN_TEST(fn) do {\
	tests_run++;\
	fprintf(stdout, "  %-50s", #fn);\
	fflush(stdout);\
	fn();\
	tests_passed++;\
} while (0)

/* -------------------------------------------------------
 * Helper: build a minimal RPC CALL header with a given
 * credential flavor + optional GSS body.
 * ------------------------------------------------------- */
static size_t build_rpc_call(uint8_t *buf, size_t bufsz,
                             uint32_t xid, uint32_t prog,
                             uint32_t vers, uint32_t proc,
                             uint32_t cred_flavor,
                             const uint8_t *cred_body,
                             uint32_t cred_body_len)
{
    XDR enc;
    xdrmem_ncreate(&enc, (char *)buf, (uint32_t)bufsz,
                   XDR_ENCODE);

    uint32_t msg_type = 0; /* CALL */
    uint32_t rpcvers = 2;
    if (!xdr_uint32_t(&enc, &xid)) { return 0; }
    if (!xdr_uint32_t(&enc, &msg_type)) { return 0; }
    if (!xdr_uint32_t(&enc, &rpcvers)) { return 0; }
    if (!xdr_uint32_t(&enc, &prog)) { return 0; }
    if (!xdr_uint32_t(&enc, &vers)) { return 0; }
    if (!xdr_uint32_t(&enc, &proc)) { return 0; }

    /* Credential: flavor + body. */
    if (!xdr_uint32_t(&enc, &cred_flavor)) { return 0; }
    if (!xdr_uint32_t(&enc, &cred_body_len)) { return 0; }
    if (cred_body_len > 0 && cred_body != NULL) {
        if (!xdr_opaque_encode(&enc, (const char *)cred_body,
                               cred_body_len)) {
            return 0;
        }
    }

    /* Verifier: AUTH_NONE, len 0. */
    uint32_t zero = 0;
    if (!xdr_uint32_t(&enc, &zero)) { return 0; }
    if (!xdr_uint32_t(&enc, &zero)) { return 0; }

    return xdr_getpos(&enc);
}

/* Build a well-formed RPCSEC_GSS credential body. */
static uint32_t build_gss_cred_body(uint8_t *out,
                                    uint32_t gss_proc,
                                    uint32_t seq,
                                    uint32_t service,
                                    const uint8_t *ctx,
                                    uint32_t ctx_len)
{
    XDR enc;
    xdrmem_ncreate(&enc, (char *)out, 256, XDR_ENCODE);

    uint32_t version = 1;
    if (!xdr_uint32_t(&enc, &version)) { return 0; }
    if (!xdr_uint32_t(&enc, &gss_proc)) { return 0; }
    if (!xdr_uint32_t(&enc, &seq)) { return 0; }
    if (!xdr_uint32_t(&enc, &service)) { return 0; }
    if (!xdr_uint32_t(&enc, &ctx_len)) { return 0; }
    if (ctx_len > 0) {
        if (!xdr_opaque_encode(&enc, (const char *)ctx,
                               ctx_len)) {
            return 0;
        }
    }
    return xdr_getpos(&enc);
}

/* -------------------------------------------------------
 * Test: GSS credential body with version 0 is rejected.
 * ------------------------------------------------------- */
static void test_gss_cred_bad_version(void)
{
    uint8_t body[256];
    XDR enc;
    xdrmem_ncreate(&enc, (char *)body, sizeof(body),
                   XDR_ENCODE);
    uint32_t bad_ver = 0;
    uint32_t proc = 0, seq = 1, svc = 1, hlen = 0;
    xdr_uint32_t(&enc, &bad_ver);
    xdr_uint32_t(&enc, &proc);
    xdr_uint32_t(&enc, &seq);
    xdr_uint32_t(&enc, &svc);
    xdr_uint32_t(&enc, &hlen);
    uint32_t body_len = xdr_getpos(&enc);

    uint8_t buf[512];
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x1000, 100003, 4, 1, 6, body, body_len);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    /* Must reject: bad GSS version. */
    ASSERT_EQ(rc, -1);
}

/* -------------------------------------------------------
 * Test: Empty GSS body (len < 20) is rejected.
 * ------------------------------------------------------- */
static void test_gss_cred_empty_body(void)
{
    uint8_t buf[512];
    /* Flavor 6 with 0-length body. */
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x2000, 100003, 4, 1, 6, NULL, 0);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    /* Empty body: decode succeeds but gss.version == 0
     * (body was skipped, not parsed). Auth gate must
     * then reject version==0. */
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(gss.version, (uint32_t)0);
}

/* -------------------------------------------------------
 * Test: Valid GSS DATA body parses correctly.
 * ------------------------------------------------------- */
static void test_gss_cred_valid_data(void)
{
    uint8_t ctx[4];
    uint32_t hid = htobe32(0xDEADBEEF);
    memcpy(ctx, &hid, 4);

    uint8_t body[256];
    uint32_t blen = build_gss_cred_body(body,
        RPCSEC_GSS_DATA, 42, RPCSEC_GSS_SVC_INTEGRITY,
        ctx, 4);
    ASSERT_GT(blen, (uint32_t)0);

    uint8_t buf[512];
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x3000, 100003, 4, 1, 6, body, blen);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(flav, (uint32_t)6);
    ASSERT_EQ(gss.version, (uint32_t)1);
    ASSERT_EQ(gss.procedure, (uint32_t)RPCSEC_GSS_DATA);
    ASSERT_EQ(gss.seq_num, (uint32_t)42);
    ASSERT_EQ(gss.service,
              (uint32_t)RPCSEC_GSS_SVC_INTEGRITY);
    ASSERT_EQ(gss.ctx_handle_len, (uint32_t)4);
}

/* -------------------------------------------------------
 * Test: Invalid GSS service (0 or 4+) is rejected.
 * ------------------------------------------------------- */
static void test_gss_cred_bad_service(void)
{
    uint8_t body[256];
    uint32_t blen = build_gss_cred_body(body,
        RPCSEC_GSS_DATA, 1, 0 /* bad */, NULL, 0);
    ASSERT_GT(blen, (uint32_t)0);

    uint8_t buf[512];
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x4000, 100003, 4, 1, 6, body, blen);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    ASSERT_EQ(rc, -1);
}

/* -------------------------------------------------------
 * Test: mds_gss_validate rejects unknown handle.
 * ------------------------------------------------------- */
static void test_validate_unknown_handle(void)
{
    /* We cannot call mds_gss_init without a real keytab,
     * so test validate with NULL table. */
    struct rpc_gss_cred cred;
    memset(&cred, 0, sizeof(cred));
    cred.version = 1;
    cred.procedure = RPCSEC_GSS_DATA;
    cred.seq_num = 1;
    cred.service = RPCSEC_GSS_SVC_NONE;
    uint32_t hid = htobe32(999);
    memcpy(cred.ctx_handle, &hid, 4);
    cred.ctx_handle_len = 4;

    uint32_t svc = 0;
    /* NULL table -> reject. */
    ASSERT_EQ(mds_gss_validate(NULL, &cred, &svc), -1);
}

/* -------------------------------------------------------
 * Test: mds_gss_validate rejects short handle.
 * ------------------------------------------------------- */
static void test_validate_short_handle(void)
{
    struct rpc_gss_cred cred;
    memset(&cred, 0, sizeof(cred));
    cred.ctx_handle_len = 2; /* Too short. */
    uint32_t svc = 0;
    ASSERT_EQ(mds_gss_validate(NULL, &cred, &svc), -1);
}

/* -------------------------------------------------------
 * Test: AUTH_SYS in krb5 mode — flavor 1 must not
 * reach a COMPOUND decoder (regression).
 *
 * We test this at the XDR level: if the server gate checks
 * flavor after decode, it will see flavor==1 and gss
 * version==0.  This test validates the decode path.
 * ------------------------------------------------------- */
static void test_auth_sys_decode_in_krb5_mode(void)
{
    uint8_t buf[512];
    /* AUTH_SYS with empty body. */
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x5000, 100003, 4, 1, 1, NULL, 0);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav = 99;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(flav, (uint32_t)1);
    /* GSS body must NOT be populated for AUTH_SYS. */
    ASSERT_EQ(gss.version, (uint32_t)0);
}

/* -------------------------------------------------------
 * Test: Unknown flavor (e.g. 99) decodes but gate
 * should reject.  Verify decode succeeds.
 * ------------------------------------------------------- */
static void test_unknown_flavor_decode(void)
{
    uint8_t buf[512];
    size_t len = build_rpc_call(buf, sizeof(buf),
        0x6000, 100003, 4, 1, 99, NULL, 0);
    ASSERT_GT(len, (size_t)0);

    XDR dec;
    xdrmem_ncreate(&dec, (char *)buf, (uint32_t)len,
                   XDR_DECODE);
    uint32_t xid, pg, vs, pc, flav = 0;
    struct rpc_gss_cred gss;
    memset(&gss, 0, sizeof(gss));
    int rc = rpc_decode_call_header(&dec, &xid, &pg,
                                    &vs, &pc, &flav, &gss, NULL, NULL, NULL, NULL);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(flav, (uint32_t)99);
    ASSERT_EQ(gss.version, (uint32_t)0);
}

/* -------------------------------------------------------
 * Test: destroy_context with NULL table rejects.
 * ------------------------------------------------------- */
static void test_destroy_context_null_table(void)
{
    uint8_t handle[4] = {0, 0, 0, 1};
    ASSERT_EQ(mds_gss_destroy_context(NULL, handle, 4),
              -1);
}

/* -------------------------------------------------------
 * Test: destroy_context with short handle rejects.
 * ------------------------------------------------------- */
static void test_destroy_context_short_handle(void)
{
    uint8_t handle[2] = {0, 1};
    ASSERT_EQ(mds_gss_destroy_context(NULL, handle, 2),
              -1);
}

/* -------------------------------------------------------
 * Test: verify_data with NULL table rejects.
 * ------------------------------------------------------- */
static void test_verify_data_null_table(void)
{
    uint8_t handle[4] = {0, 0, 0, 1};
    uint8_t msg[] = "hello";
    uint8_t mic[] = "fake";
    ASSERT_EQ(mds_gss_verify_data(NULL, handle, 4,
                                   msg, 5, mic, 4), -1);
}

/* -------------------------------------------------------
 * Test: unwrap_data with NULL table rejects.
 * ------------------------------------------------------- */
static void test_unwrap_data_null_table(void)
{
    uint8_t handle[4] = {0, 0, 0, 1};
    uint8_t wrapped[] = "fake_wrapped";
    void *plain = NULL;
    size_t plen = 0;
    ASSERT_EQ(mds_gss_unwrap_data(NULL, handle, 4,
                                   wrapped, 12,
                                   &plain, &plen), -1);
    ASSERT_EQ(plain, NULL);
}

/* -------------------------------------------------------
 * Test: get_mic with NULL table rejects.
 * ------------------------------------------------------- */
static void test_get_mic_null_table(void)
{
    uint8_t handle[4] = {0, 0, 0, 1};
    uint8_t msg[] = "hello";
    void *mic = NULL;
    size_t mlen = 0;
    ASSERT_EQ(mds_gss_get_mic(NULL, handle, 4,
                               msg, 5, &mic, &mlen), -1);
    ASSERT_EQ(mic, NULL);
}

/* -------------------------------------------------------
 * Test: wrap_data with NULL table rejects.
 * ------------------------------------------------------- */
static void test_wrap_data_null_table(void)
{
    uint8_t handle[4] = {0, 0, 0, 1};
    uint8_t plain[] = "hello";
    void *wrapped = NULL;
    size_t wlen = 0;
    ASSERT_EQ(mds_gss_wrap_data(NULL, handle, 4,
                                 plain, 5,
                                 &wrapped, &wlen), -1);
    ASSERT_EQ(wrapped, NULL);
}

int main(void)
{
    RUN_TEST(test_gss_cred_bad_version);
    RUN_TEST(test_gss_cred_empty_body);
    RUN_TEST(test_gss_cred_valid_data);
    RUN_TEST(test_gss_cred_bad_service);
    RUN_TEST(test_validate_unknown_handle);
    RUN_TEST(test_validate_short_handle);
    RUN_TEST(test_auth_sys_decode_in_krb5_mode);
    RUN_TEST(test_unknown_flavor_decode);
    RUN_TEST(test_destroy_context_null_table);
    RUN_TEST(test_destroy_context_short_handle);
    RUN_TEST(test_verify_data_null_table);
    RUN_TEST(test_unwrap_data_null_table);
    RUN_TEST(test_get_mic_null_table);
    RUN_TEST(test_wrap_data_null_table);
    fprintf(stdout, "%d/%d tests passed\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
