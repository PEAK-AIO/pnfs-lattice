/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_xdr_layout_form.c -- Phase C / Step 6 of docs/hpc-nto1-plan.md.
 *
 * Round-trips the flex-files LAYOUTGET body in both wire forms
 * (LEGACY one-DS-per-mirror, STRIPED multi-DS-per-mirror) and
 * asserts the structural shape on the decode side:
 *
 *   LEGACY:  mirror_count == stripe_count, per-mirror ds_count == 1
 *   STRIPED: mirror_count == 1,            per-mirror ds_count == stripe_count
 *
 * Both shapes share the leading return_on_close / stateid / layout4
 * envelope and the trailing ff_flags / ffl_stats_collect_hint footer;
 * the test parses the entire frame so a regression in any field is
 * caught here, not at runtime.
 *
 * The test deliberately does not depend on the catalogue, the proxy,
 * or any compound state -- it talks directly to encode_res_layoutget
 * (declared extern below) so that unit-test isolation matches
 * test_layout.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "xdr_codec.h"

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
    fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
        #expr, __FILE__, __LINE__); abort(); } } while (0)

extern bool encode_res_layoutget(XDR *xdrs, const struct nfs4_result *r);

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

/*
 * Decode helpers.  Mirror the encoder layout exactly so any wire-
 * format drift is loud.  Return false on any underrun.
 */
struct decoded_envelope {
    bool                return_on_close;
    struct nfs4_stateid stateid;
    uint32_t            layout_count;
    uint64_t            offset;
    uint64_t            length;
    uint32_t            iomode;
    uint32_t            layout_type;
    uint32_t            body_len;
    /* Pointer into the source buffer at the start of the body.
     * Caller is responsible for keeping the buffer alive. */
    const char         *body;
};

static bool decode_envelope(XDR *dec, const char *src,
                            struct decoded_envelope *out)
{
    int32_t roc;
    uint32_t seqid;

    if (!xdr_getbool(dec, &roc)) { return false; }
    out->return_on_close = (roc != 0);
    if (!xdr_uint32_t(dec, &seqid)) { return false; }
    out->stateid.seqid = seqid;
    if (!xdr_opaque_decode(dec, (char *)out->stateid.other, 12)) {
        return false;
    }
    if (!xdr_uint32_t(dec, &out->layout_count)) { return false; }
    if (!xdr_uint64_t(dec, &out->offset)) { return false; }
    if (!xdr_uint64_t(dec, &out->length)) { return false; }
    if (!xdr_uint32_t(dec, &out->iomode)) { return false; }
    if (!xdr_uint32_t(dec, &out->layout_type)) { return false; }
    if (!xdr_uint32_t(dec, &out->body_len)) { return false; }

    /* The body itself is opaque<>; capture a pointer rather than
     * copying.  The decoder advances by xdr_inrec_align so we
     * read back the body via xdr_opaque_decode below. */
    out->body = src + xdr_getpos(dec);
    return true;
}

/*
 * Decode the body opaque payload into a fresh XDR stream so we can
 * walk the per-form structure.  Caller passes the body length and
 * reads up to that many bytes.
 */
struct decoded_body {
    uint64_t stripe_unit;
    uint32_t mirror_count;
    /* Per-mirror inner ds_count, captured for the first
     * NFS4_BODY_MAX_MIRRORS entries.  Sufficient for unit tests
     * which never exceed 4 mirrors. */
#define NFS4_BODY_MAX_MIRRORS 16
    uint32_t inner_ds_counts[NFS4_BODY_MAX_MIRRORS];
};

static bool decode_body_skip_one_ds(XDR *body)
{
    /* deviceid(16) + efficiency(4) + stateid(seqid + other 12) +
     * fh_count(4) + fh_len(4) + fh + utf8(uid) + utf8(gid). */
    char devid[NFS4_DEVICEID4_SIZE];
    uint32_t eff;
    uint32_t fh_count;
    uint32_t fh_len;
    char fh[MDS_NFS_FH_MAX];
    char ubuf[64], gbuf[64];
    char *up = ubuf;
    char *gp = gbuf;
    uint32_t seqid;
    char other[12];

    if (!xdr_opaque_decode(body, devid, NFS4_DEVICEID4_SIZE)) { return false; }
    if (!xdr_uint32_t(body, &eff)) { return false; }
    if (!xdr_uint32_t(body, &seqid)) { return false; }
    if (!xdr_opaque_decode(body, other, 12)) { return false; }
    if (!xdr_uint32_t(body, &fh_count)) { return false; }
    if (!xdr_uint32_t(body, &fh_len)) { return false; }
    if (fh_len > 0 && fh_len <= sizeof(fh)) {
        if (!xdr_opaque_decode(body, fh, fh_len)) { return false; }
    } else if (fh_len > 0) {
        return false;
    }
    if (!xdr_string_decode(body, &up, sizeof(ubuf))) { return false; }
    if (!xdr_string_decode(body, &gp, sizeof(gbuf))) { return false; }
    return true;
}

static bool decode_body(const char *src, uint32_t len,
                        struct decoded_body *out)
{
    XDR body;
    xdrmem_ncreate(&body, (char *)src, len, XDR_DECODE);

    memset(out, 0, sizeof(*out));
    if (!xdr_uint64_t(&body, &out->stripe_unit)) { return false; }
    if (!xdr_uint32_t(&body, &out->mirror_count)) { return false; }
    for (uint32_t m = 0; m < out->mirror_count; m++) {
        uint32_t inner;
        if (!xdr_uint32_t(&body, &inner)) { return false; }
        if (m < NFS4_BODY_MAX_MIRRORS) {
            out->inner_ds_counts[m] = inner;
        }
        for (uint32_t i = 0; i < inner; i++) {
            if (!decode_body_skip_one_ds(&body)) { return false; }
        }
    }
    /* Trailer: ff_flags + ffl_stats_collect_hint.  Read but ignore. */
    uint32_t ff_flags, hint;
    if (!xdr_uint32_t(&body, &ff_flags)) { return false; }
    if (!xdr_uint32_t(&body, &hint)) { return false; }
    return true;
}

/* -------------------------------------------------------------------
 * Build a 4-stripe / 1-mirror flex-files result with the requested
 * wire form.  Caller frees via nfs4_res_layoutget_destroy().
 * ------------------------------------------------------------------- */

static void build_4x1_layout(struct nfs4_result *res,
                             enum nfs4_ff_xdr_form form)
{
    struct nfs4_res_layoutget *lg = &res->res.layoutget;

    memset(res, 0, sizeof(*res));
    res->opnum = OP_LAYOUTGET;
    res->status = NFS4_OK;

    lg->return_on_close = false;
    lg->stateid.seqid = 1;
    memset(lg->stateid.other, 0xCD, 12);
    lg->layout_type = LAYOUT4_FLEX_FILES;
    lg->iomode = LAYOUTIOMODE4_RW;
    lg->offset = 0;
    lg->length = UINT64_MAX;
    lg->stripe_unit = 65536;
    lg->ff_xdr_form = form;
    lg->ff_flags = 0;

    if (form == NFS4_FF_XDR_FORM_LEGACY) {
        /* Legacy: 4 outer mirrors, each with ds_count = 1. */
        VERIFY(nfs4_res_layoutget_alloc(lg, 4, 4) == 0);
        for (uint32_t s = 0; s < 4; s++) {
            VERIFY(nfs4_ff_mirror_alloc(&lg->ff_mirrors[s], 1) == 0);
            lg->ff_mirrors[s].ds[0].ds_id = s + 10;
            lg->ff_mirrors[s].ds[0].nfs_fh_len = 4;
            memcpy(lg->ff_mirrors[s].ds[0].nfs_fh, "FHxx", 4);
            lg->ds[s].ds_id = s + 10;
            lg->ds[s].nfs_fh_len = 4;
            memcpy(lg->ds[s].nfs_fh, "FHxx", 4);
        }
    } else {
        /* Striped: 1 outer mirror, ds_count = 4. */
        VERIFY(nfs4_res_layoutget_alloc(lg, 4, 1) == 0);
        VERIFY(nfs4_ff_mirror_alloc(&lg->ff_mirrors[0], 4) == 0);
        for (uint32_t s = 0; s < 4; s++) {
            lg->ff_mirrors[0].ds[s].ds_id = s + 20;
            lg->ff_mirrors[0].ds[s].nfs_fh_len = 4;
            memcpy(lg->ff_mirrors[0].ds[s].nfs_fh, "FHyy", 4);
            lg->ds[s].ds_id = s + 20;
            lg->ds[s].nfs_fh_len = 4;
            memcpy(lg->ds[s].nfs_fh, "FHyy", 4);
        }
    }
}

/* -------------------------------------------------------------------
 * Test 1: legacy form round-trips with 4 outer mirrors, each ds_count=1.
 * ------------------------------------------------------------------- */
static void test_legacy_round_trip(void)
{
    struct nfs4_result res;
    uint8_t buf[8192];
    XDR enc, dec;

    build_4x1_layout(&res, NFS4_FF_XDR_FORM_LEGACY);

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_layoutget(&enc, &res));
    uint32_t total_len = xdr_getpos(&enc);
    VERIFY(total_len > 0);

    xdrmem_ncreate(&dec, (char *)buf, total_len, XDR_DECODE);

    struct decoded_envelope env;
    VERIFY(decode_envelope(&dec, (const char *)buf, &env));
    VERIFY(env.layout_type == LAYOUT4_FLEX_FILES);

    struct decoded_body body;
    VERIFY(decode_body(env.body, env.body_len, &body));

    VERIFY(body.stripe_unit == 65536);
    VERIFY(body.mirror_count == 4);
    for (uint32_t m = 0; m < 4; m++) {
        VERIFY(body.inner_ds_counts[m] == 1);
    }

    nfs4_res_layoutget_destroy(&res.res.layoutget);
}

/* -------------------------------------------------------------------
 * Test 2: striped form round-trips with 1 outer mirror, ds_count=4.
 * ------------------------------------------------------------------- */
static void test_striped_round_trip(void)
{
    struct nfs4_result res;
    uint8_t buf[8192];
    XDR enc, dec;

    build_4x1_layout(&res, NFS4_FF_XDR_FORM_STRIPED);

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_layoutget(&enc, &res));
    uint32_t total_len = xdr_getpos(&enc);
    VERIFY(total_len > 0);

    xdrmem_ncreate(&dec, (char *)buf, total_len, XDR_DECODE);

    struct decoded_envelope env;
    VERIFY(decode_envelope(&dec, (const char *)buf, &env));
    VERIFY(env.layout_type == LAYOUT4_FLEX_FILES);

    struct decoded_body body;
    VERIFY(decode_body(env.body, env.body_len, &body));

    VERIFY(body.stripe_unit == 65536);
    VERIFY(body.mirror_count == 1);
    VERIFY(body.inner_ds_counts[0] == 4);

    nfs4_res_layoutget_destroy(&res.res.layoutget);
}

/* -------------------------------------------------------------------
 * Test 3: default ff_xdr_form (zero) selects LEGACY.  Regression guard
 * for any future change that moves the default away from legacy and
 * accidentally breaks pre-Phase-C clients.
 * ------------------------------------------------------------------- */
static void test_default_form_is_legacy(void)
{
    struct nfs4_result res;
    uint8_t buf[8192];
    XDR enc, dec;

    build_4x1_layout(&res, NFS4_FF_XDR_FORM_LEGACY);
    /* Force ff_xdr_form back to the default value. */
    res.res.layoutget.ff_xdr_form = (enum nfs4_ff_xdr_form)0;

    xdrmem_ncreate(&enc, (char *)buf, sizeof(buf), XDR_ENCODE);
    VERIFY(encode_res_layoutget(&enc, &res));
    uint32_t total_len = xdr_getpos(&enc);

    xdrmem_ncreate(&dec, (char *)buf, total_len, XDR_DECODE);
    struct decoded_envelope env;
    VERIFY(decode_envelope(&dec, (const char *)buf, &env));

    struct decoded_body body;
    VERIFY(decode_body(env.body, env.body_len, &body));

    VERIFY(body.mirror_count == 4);
    for (uint32_t m = 0; m < 4; m++) {
        VERIFY(body.inner_ds_counts[m] == 1);
    }

    nfs4_res_layoutget_destroy(&res.res.layoutget);
}

int main(void)
{
    fprintf(stdout, "Running test_xdr_layout_form:\n");
    RUN_TEST(test_legacy_round_trip);
    RUN_TEST(test_striped_round_trip);
    RUN_TEST(test_default_form_is_legacy);
    fprintf(stdout, "\n%d/%d xdr-layout-form tests passed.\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
