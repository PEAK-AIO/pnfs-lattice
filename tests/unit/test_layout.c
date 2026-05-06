/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_layout.c -- Unit tests for flex file layout XDR encoding
 * and DELEGRETURN dispatch.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "xdr_codec.h"

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
    fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
        #expr, __FILE__, __LINE__); abort(); } } while (0)

/* Forward declarations for internal XDR encoders. */
extern bool encode_res_layoutget(XDR *xdrs, const struct nfs4_result *r);
extern bool encode_res_getdeviceinfo(XDR *xdrs, const struct nfs4_result *r);

static int tests_run;
static int tests_passed;

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* -------------------------------------------------------------------
 * Test 1: encode_res_layoutget -- single-stripe layout
 * ------------------------------------------------------------------- */
static void test_layout_encode_single_stripe(void)
{
    struct nfs4_result res;
    struct nfs4_res_layoutget *lg = &res.res.layoutget;
    uint8_t buf[4096];
    XDR enc, dec;

    memset(&res, 0, sizeof(res));
    res.opnum = OP_LAYOUTGET;
    res.status = NFS4_OK;

    lg->return_on_close = false;
    lg->stateid.seqid = 1;
    memset(lg->stateid.other, 0xAB, 12);
    lg->layout_type = LAYOUT4_FLEX_FILES;
    lg->iomode = 2; /* RW */
    lg->offset = 0;
    lg->length = UINT64_MAX;
    lg->stripe_unit = 65536;
    /* Phase C / Step 1: heap-ified wire buffers. */
    VERIFY(nfs4_res_layoutget_alloc(lg, 1, 1) == 0);
    VERIFY(nfs4_ff_mirror_alloc(&lg->ff_mirrors[0], 1) == 0);
    lg->ds[0].ds_id = 1;
    memset(lg->ds[0].deviceid, 0, 16);
    lg->ds[0].deviceid[3] = 1;
    lg->ds[0].deviceid[7] = 1;
    lg->ds[0].nfs_fh_len = 4;
    memcpy(lg->ds[0].nfs_fh, "fh01", 4);

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_layoutget(&enc, &res));
    uint32_t len = xdr_getpos(&enc);
    VERIFY(len > 0);

    /* Decode: return_on_close + stateid. */
    xdrmem_ncreate(&dec, (char *)buf, len, XDR_DECODE);
    int32_t roc;
    VERIFY(xdr_getbool(&dec, &roc));
    VERIFY(roc == 0);

    uint32_t seqid;
    uint8_t other[12];
    VERIFY(xdr_uint32_t(&dec, &seqid));
    VERIFY(seqid == 1);
    VERIFY(xdr_opaque_decode(&dec, (char *)other, 12));
    VERIFY(other[0] == 0xAB);

    nfs4_res_layoutget_destroy(lg);
}

/* -------------------------------------------------------------------
 * Test 2: encode_res_layoutget -- mirrored (2 DS)
 * ------------------------------------------------------------------- */
static void test_layout_encode_mirrored(void)
{
    struct nfs4_result res;
    struct nfs4_res_layoutget *lg = &res.res.layoutget;
    uint8_t buf[8192];
    XDR enc, dec;

    memset(&res, 0, sizeof(res));
    res.opnum = OP_LAYOUTGET;
    res.status = NFS4_OK;

    lg->return_on_close = true;
    lg->stateid.seqid = 2;
    lg->layout_type = LAYOUT4_FLEX_FILES;
    lg->iomode = 2;
    lg->offset = 0;
    lg->length = UINT64_MAX;
    /* Phase C / Step 1: heap-ified wire buffers. */
    VERIFY(nfs4_res_layoutget_alloc(lg, 2, 2) == 0);
    VERIFY(nfs4_ff_mirror_alloc(&lg->ff_mirrors[0], 1) == 0);
    VERIFY(nfs4_ff_mirror_alloc(&lg->ff_mirrors[1], 1) == 0);
    for (int i = 0; i < 2; i++) {
        lg->ds[i].ds_id = (uint32_t)(i + 1);
        memset(lg->ds[i].deviceid, 0, 16);
        lg->ds[i].deviceid[7] = (uint8_t)(i + 1);
        lg->ds[i].nfs_fh_len = 4;
        memcpy(lg->ds[i].nfs_fh, "fhXX", 4);
    }

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_layoutget(&enc, &res));
    VERIFY(xdr_getpos(&enc) > 0);

    xdrmem_ncreate(&dec, (char *)buf, xdr_getpos(&enc), XDR_DECODE);
    int32_t roc;
    VERIFY(xdr_getbool(&dec, &roc));
    VERIFY(roc == 1);

    nfs4_res_layoutget_destroy(lg);
}

/* -------------------------------------------------------------------
 * Test 3: encode_res_getdeviceinfo
 * ------------------------------------------------------------------- */
static void test_layout_encode_deviceinfo(void)
{
    struct nfs4_result res;
    struct nfs4_res_getdeviceinfo *gdi = &res.res.getdeviceinfo;
    uint8_t buf[4096];
    XDR enc;

    memset(&res, 0, sizeof(res));
    res.opnum = OP_GETDEVICEINFO;
    res.status = NFS4_OK;

    gdi->layout_type = LAYOUT4_FLEX_FILES;
    gdi->ds_count = 1;
    gdi->ds[0].ds_id = 1;
    gdi->ds[0].port = 2049;
    snprintf(gdi->ds[0].addr, sizeof(gdi->ds[0].addr), "10.0.0.1");

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_getdeviceinfo(&enc, &res));
    VERIFY(xdr_getpos(&enc) > 0);
}

/* -------------------------------------------------------------------
 * Test 4: DELEGRETURN dispatch returns NFS4_OK
 * ------------------------------------------------------------------- */
static void test_delegreturn_dispatch(void)
{
    struct compound_data cd;
    struct nfs4_op ops[1];
    struct nfs4_result res[1];

    compound_init(&cd);
    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_DELEGRETURN;

    uint32_t n = compound_process(&cd, ops, res, 1);
    VERIFY(n == 1);
    VERIFY(res[0].status == NFS4_OK);
}

/* -------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------- */
int main(void)
{
    fprintf(stdout, "Running layout tests:\n");
    RUN_TEST(test_layout_encode_single_stripe);
    RUN_TEST(test_layout_encode_mirrored);
    RUN_TEST(test_layout_encode_deviceinfo);
    RUN_TEST(test_delegreturn_dispatch);
    fprintf(stdout, "\n%d/%d layout tests passed.\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
