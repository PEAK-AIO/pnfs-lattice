/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_gss.c — RPCSEC_GSS context management.
 */

#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <time.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "mds_gss.h"

#define MAX_GSS_CONTEXTS 256

struct gss_ctx_entry {
    uint32_t     handle_id;
    gss_ctx_id_t gss_ctx;
    uint32_t     negotiated_svc; /**< Server-side negotiated service. */
    uint32_t     seq_window_low; /**< Bottom of sequence window. */
    uint64_t     seq_seen;       /**< Bitmap of seen seqs in window. */
    bool         established;
    time_t       last_used;      /**< Monotonic timestamp of last validate. */
};

struct mds_gss_table {
    gss_cred_id_t  server_cred;
    struct gss_ctx_entry contexts[MAX_GSS_CONTEXTS];
};

int mds_gss_init(const char *keytab_path, const char *principal,
                 struct mds_gss_table **out)
{
    if (out == NULL) {
        return -1;
    }

    /* Set keytab environment if provided. */
    if (keytab_path != NULL && keytab_path[0] != '\0') {
        /* KRB5_KTNAME is the standard way to point GSSAPI at a keytab. */
        (void)setenv( /* NOLINT(concurrency-mt-unsafe) */
            "KRB5_KTNAME", keytab_path, 1);
    }

    struct mds_gss_table *tbl = calloc(1, sizeof(*tbl));
    if (tbl == NULL) {
        return -1;
    }

    /* Acquire server credentials. */
    OM_uint32 major, minor;
    gss_name_t server_name = GSS_C_NO_NAME;

    if (principal != NULL && principal[0] != '\0') {
        gss_buffer_desc name_buf;
        name_buf.value = (void *)principal;
        name_buf.length = strlen(principal);

        major = gss_import_name(&minor, &name_buf,
                                GSS_C_NT_HOSTBASED_SERVICE,
                                &server_name);
        if (GSS_ERROR(major)) {
            free(tbl);
            return -1;
        }
    }

    major = gss_acquire_cred(&minor, server_name, GSS_C_INDEFINITE,
                              GSS_C_NO_OID_SET, GSS_C_ACCEPT,
                              &tbl->server_cred, NULL, NULL);

    if (server_name != GSS_C_NO_NAME) {
        OM_uint32 rel_minor;
        (void)gss_release_name(&rel_minor, &server_name);
    }

    if (GSS_ERROR(major)) {
        free(tbl);
        return -1;
    }

    *out = tbl;
    return 0;
}


uint32_t mds_gss_evict_idle(struct mds_gss_table *tbl,
                            uint32_t max_idle_sec)
{
    if (tbl == NULL) {
        return 0;
    }

    time_t now = time(NULL);
    uint32_t evicted = 0;

    for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
        struct gss_ctx_entry *e = &tbl->contexts[i];
        if (!e->established) {
            continue;
        }
        if (now - e->last_used > (time_t)max_idle_sec) {
            OM_uint32 minor;
            if (e->gss_ctx != GSS_C_NO_CONTEXT) {
                (void)gss_delete_sec_context(
                    &minor, &e->gss_ctx,
                    GSS_C_NO_BUFFER);
            }
            memset(e, 0, sizeof(*e));
            evicted++;
        }
    }
    return evicted;
}

void mds_gss_destroy(struct mds_gss_table *tbl)
{
    if (tbl == NULL) {
        return;
    }

    OM_uint32 minor;

    for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
        if (tbl->contexts[i].gss_ctx != GSS_C_NO_CONTEXT) {
            (void)gss_delete_sec_context(&minor,
                &tbl->contexts[i].gss_ctx, GSS_C_NO_BUFFER);
        }
    }

    if (tbl->server_cred != GSS_C_NO_CREDENTIAL) {
        (void)gss_release_cred(&minor, &tbl->server_cred);
    }

    free(tbl);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int mds_gss_accept_token(struct mds_gss_table *tbl,
                         uint32_t gss_proc,
                         const uint8_t *existing_handle,
                         uint32_t existing_handle_len,
                         const void *in_token, size_t in_len,
                         void **out_token, size_t *out_len,
                         uint8_t *ctx_handle, uint32_t *ctx_len,
                         bool *complete)
{
    if (tbl == NULL || out_token == NULL || out_len == NULL ||
        ctx_handle == NULL || ctx_len == NULL ||
        complete == NULL) {
        return -1;
    }

    *out_token = NULL;
    *out_len = 0;
    *complete = false;

    uint32_t slot = UINT32_MAX;

    if (gss_proc == RPCSEC_GSS_CONTINUE_INIT &&
            existing_handle != NULL &&
            existing_handle_len >= 4) {
        /* Resume existing context for multi-leg. */
        uint32_t hid_be;
        memcpy(&hid_be, existing_handle, 4);
        uint32_t hid = be32toh(hid_be);
        for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
            if (tbl->contexts[i].handle_id == hid &&
                    !tbl->contexts[i].established) {
                slot = i;
                break;
            }
        }
        if (slot == UINT32_MAX) {
            return -1; /* Handle not found. */
        }
    } else {
        /* INIT: find a free slot. */
        for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
            if (!tbl->contexts[i].established &&
                tbl->contexts[i].gss_ctx ==
                    GSS_C_NO_CONTEXT) {
                slot = i;
                break;
            }
        }
        if (slot == UINT32_MAX) {
            return -1; /* No free slots. */
        }
    }

    gss_buffer_desc in_buf = { .length = in_len,
                               .value = (void *)in_token };
    gss_buffer_desc out_buf = GSS_C_EMPTY_BUFFER;

    OM_uint32 major, minor;
    OM_uint32 ret_flags = 0;
    major = gss_accept_sec_context(
        &minor,
        &tbl->contexts[slot].gss_ctx,
        tbl->server_cred,
        &in_buf,
        GSS_C_NO_CHANNEL_BINDINGS,
        NULL,  /* src_name */
        NULL,  /* mech_type */
        &out_buf,
        &ret_flags,
        NULL,  /* time_rec */
        NULL   /* delegated_cred */
    );

    if (GSS_ERROR(major)) {
        return -1;
    }

    if (out_buf.length > 0) {
        *out_token = malloc(out_buf.length);
        if (*out_token == NULL) {
            OM_uint32 rel;
            (void)gss_release_buffer(&rel, &out_buf);
            return -1;
        }
        memcpy(*out_token, out_buf.value, out_buf.length);
        *out_len = out_buf.length;
        OM_uint32 rel;
        (void)gss_release_buffer(&rel, &out_buf);
    }

    if (gss_proc == RPCSEC_GSS_CONTINUE_INIT) {
        /* Return existing handle. */
        uint32_t hid_be = htobe32(
            tbl->contexts[slot].handle_id);
        memcpy(ctx_handle, &hid_be, 4);
        *ctx_len = 4;
    } else {
        /* INIT: generate random handle. */
        uint32_t hid;
        if (getrandom(&hid, sizeof(hid), 0) !=
                (ssize_t)sizeof(hid)) {
            return -1;
        }
        if (hid == 0) {
            hid = 1;
        }
        tbl->contexts[slot].handle_id = hid;
        uint32_t hid_be = htobe32(hid);
        memcpy(ctx_handle, &hid_be, 4);
        *ctx_len = 4;
    }

    if (major == GSS_S_COMPLETE) {
        tbl->contexts[slot].established = true;
        /* Derive negotiated service from ret_flags. */
        if (ret_flags & GSS_C_CONF_FLAG) {
            tbl->contexts[slot].negotiated_svc =
                RPCSEC_GSS_SVC_PRIVACY;
        } else if (ret_flags & GSS_C_INTEG_FLAG) {
            tbl->contexts[slot].negotiated_svc =
                RPCSEC_GSS_SVC_INTEGRITY;
        } else {
            tbl->contexts[slot].negotiated_svc =
                RPCSEC_GSS_SVC_NONE;
        }
        tbl->contexts[slot].seq_window_low = 0;
        *complete = true;
    }

    return 0;
}

int mds_gss_destroy_context(struct mds_gss_table *tbl,
                           const uint8_t *ctx_handle,
                           uint32_t ctx_handle_len)
{
    if (tbl == NULL || ctx_handle == NULL ||
            ctx_handle_len < 4) {
        return -1;
    }

    uint32_t hid_be;
    memcpy(&hid_be, ctx_handle, 4);
    uint32_t hid = be32toh(hid_be);

    for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
        if (tbl->contexts[i].handle_id == hid &&
                tbl->contexts[i].established) {
            OM_uint32 minor;
            if (tbl->contexts[i].gss_ctx !=
                    GSS_C_NO_CONTEXT) {
                (void)gss_delete_sec_context(
                    &minor,
                    &tbl->contexts[i].gss_ctx,
                    GSS_C_NO_BUFFER);
            }
            memset(&tbl->contexts[i], 0,
                   sizeof(tbl->contexts[i]));
            return 0;
        }
    }

    return -1; /* Not found. */
}

int mds_gss_validate(struct mds_gss_table *tbl,
                     const struct rpc_gss_cred *cred,
                     uint32_t *service)
{
    if (tbl == NULL || cred == NULL || service == NULL) {
        return -1;
    }

    if (cred->ctx_handle_len < 4) {
        return -1;
    }

    uint32_t hid_be;
    memcpy(&hid_be, cred->ctx_handle, 4);
    uint32_t hid = be32toh(hid_be);

    for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
        if (tbl->contexts[i].handle_id == hid &&
            tbl->contexts[i].established) {
            /* Sequence window check (RFC 2203 s5.3.3.1).
             * Accept if seq_num is within [low+1, low+64]
             * and not already seen. Track seen bits in a
             * 64-bit bitmap. */
            uint32_t low =
                tbl->contexts[i].seq_window_low;
            uint32_t seq = cred->seq_num;
            if (seq <= low) {
                return -1; /* Below window. */
            }
            uint32_t offset = seq - low;
            if (offset > 64) {
                /* Advance window. */
                uint32_t shift = offset - 64;
                if (shift >= 64) {
                    tbl->contexts[i].seq_seen = 0;
                } else {
                    tbl->contexts[i].seq_seen >>=
                        shift;
                }
                tbl->contexts[i].seq_window_low =
                    seq - 64;
                low = tbl->contexts[i].seq_window_low;
                offset = seq - low;
            }
            uint64_t bit = (uint64_t)1
                << (offset - 1);
            if (tbl->contexts[i].seq_seen & bit) {
                return -1; /* Replay. */
            }
            tbl->contexts[i].seq_seen |= bit;

            /* Return server-negotiated service, NOT the
             * client-asserted cred->service.  The client
             * may claim krb5p but the server only
             * negotiated krb5. */
            uint32_t svc =
                tbl->contexts[i].negotiated_svc;
            /* If client requests a higher service than
             * negotiated, cap to negotiated level. */
            if (cred->service > svc) {
                *service = svc;
            } else {
                *service = cred->service;
            }
            return 0;
        }
    }

    return -1;  /* Unknown or unestablished context. */
}

/* -------------------------------------------------------
 * Internal: look up gss_ctx_id_t from handle bytes.
 * ------------------------------------------------------- */
static gss_ctx_id_t find_gss_ctx(struct mds_gss_table *tbl,
                                  const uint8_t *handle,
                                  uint32_t handle_len)
{
    if (tbl == NULL || handle == NULL || handle_len < 4) {
        return GSS_C_NO_CONTEXT;
    }
    uint32_t hid_be;
    memcpy(&hid_be, handle, 4);
    uint32_t hid = be32toh(hid_be);
    for (uint32_t i = 0; i < MAX_GSS_CONTEXTS; i++) {
        if (tbl->contexts[i].handle_id == hid &&
                tbl->contexts[i].established) {
            return tbl->contexts[i].gss_ctx;
        }
    }
    return GSS_C_NO_CONTEXT;
}

/* -------------------------------------------------------
 * krb5i request path: verify MIC on message body.
 * ------------------------------------------------------- */
int mds_gss_verify_header(struct mds_gss_table *tbl,
                          const struct rpc_gss_cred *cred,
                          const void *header_bytes,
                          size_t header_len)
{
    if (tbl == NULL || cred == NULL ||
            header_bytes == NULL || header_len == 0) {
        return -1;
    }

    gss_ctx_id_t ctx = find_gss_ctx(
        tbl, cred->ctx_handle, cred->ctx_handle_len);
    if (ctx == GSS_C_NO_CONTEXT) {
        return -1;
    }

    /* RFC 2203 s5.3.1: verifier = MIC of RPC header
     * from XID through credential (inclusive). */
    gss_buffer_desc msg_buf = {
        .length = header_len,
        .value  = (void *)header_bytes
    };
    gss_buffer_desc mic_buf = {
        .length = cred->verf_body_len,
        .value  = (void *)cred->verf_body
    };

    OM_uint32 major, minor;
    major = gss_verify_mic(
        &minor, ctx, &msg_buf, &mic_buf, NULL);
    return GSS_ERROR(major) ? -1 : 0;
}

int mds_gss_verify_data(struct mds_gss_table *tbl,
                        const uint8_t *ctx_handle,
                        uint32_t ctx_handle_len,
                        const void *message,
                        size_t message_len,
                        const void *mic_token,
                        size_t mic_token_len)
{
    gss_ctx_id_t ctx = find_gss_ctx(
        tbl, ctx_handle, ctx_handle_len);
    if (ctx == GSS_C_NO_CONTEXT) {
        return -1;
    }

    gss_buffer_desc msg_buf = {
        .length = message_len,
        .value  = (void *)message
    };
    gss_buffer_desc mic_buf = {
        .length = mic_token_len,
        .value  = (void *)mic_token
    };

    OM_uint32 major, minor;
    major = gss_verify_mic(
        &minor, ctx, &msg_buf, &mic_buf, NULL);
    return GSS_ERROR(major) ? -1 : 0;
}

/* -------------------------------------------------------
 * krb5i reply path: compute MIC over message body.
 * ------------------------------------------------------- */
int mds_gss_get_mic(struct mds_gss_table *tbl,
                    const uint8_t *ctx_handle,
                    uint32_t ctx_handle_len,
                    const void *message,
                    size_t message_len,
                    void **mic_out,
                    size_t *mic_out_len)
{
    if (mic_out == NULL || mic_out_len == NULL) {
        return -1;
    }
    *mic_out = NULL;
    *mic_out_len = 0;

    gss_ctx_id_t ctx = find_gss_ctx(
        tbl, ctx_handle, ctx_handle_len);
    if (ctx == GSS_C_NO_CONTEXT) {
        return -1;
    }

    gss_buffer_desc msg_buf = {
        .length = message_len,
        .value  = (void *)message
    };
    gss_buffer_desc mic_buf = GSS_C_EMPTY_BUFFER;

    OM_uint32 major, minor;
    major = gss_get_mic(&minor, ctx, GSS_C_QOP_DEFAULT,
                        &msg_buf, &mic_buf);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *mic_out = malloc(mic_buf.length);
    if (*mic_out == NULL) {
        OM_uint32 rel;
        (void)gss_release_buffer(&rel, &mic_buf);
        return -1;
    }
    memcpy(*mic_out, mic_buf.value, mic_buf.length);
    *mic_out_len = mic_buf.length;
    OM_uint32 rel;
    (void)gss_release_buffer(&rel, &mic_buf);
    return 0;
}

/* -------------------------------------------------------
 * krb5p request path: unwrap (decrypt) message body.
 * ------------------------------------------------------- */
int mds_gss_unwrap_data(struct mds_gss_table *tbl,
                        const uint8_t *ctx_handle,
                        uint32_t ctx_handle_len,
                        const void *wrapped,
                        size_t wrapped_len,
                        void **plaintext,
                        size_t *plaintext_len)
{
    if (plaintext == NULL || plaintext_len == NULL) {
        return -1;
    }
    *plaintext = NULL;
    *plaintext_len = 0;

    gss_ctx_id_t ctx = find_gss_ctx(
        tbl, ctx_handle, ctx_handle_len);
    if (ctx == GSS_C_NO_CONTEXT) {
        return -1;
    }

    gss_buffer_desc in_buf = {
        .length = wrapped_len,
        .value  = (void *)wrapped
    };
    gss_buffer_desc out_buf = GSS_C_EMPTY_BUFFER;

    OM_uint32 major, minor;
    int conf_state = 0;
    major = gss_unwrap(&minor, ctx, &in_buf,
                       &out_buf, &conf_state, NULL);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *plaintext = malloc(out_buf.length);
    if (*plaintext == NULL) {
        OM_uint32 rel;
        (void)gss_release_buffer(&rel, &out_buf);
        return -1;
    }
    memcpy(*plaintext, out_buf.value, out_buf.length);
    *plaintext_len = out_buf.length;
    OM_uint32 rel;
    (void)gss_release_buffer(&rel, &out_buf);
    return 0;
}

/* -------------------------------------------------------
 * krb5p reply path: wrap (encrypt) message body.
 * ------------------------------------------------------- */
int mds_gss_wrap_data(struct mds_gss_table *tbl,
                      const uint8_t *ctx_handle,
                      uint32_t ctx_handle_len,
                      const void *plaintext,
                      size_t plaintext_len,
                      void **wrapped,
                      size_t *wrapped_len)
{
    if (wrapped == NULL || wrapped_len == NULL) {
        return -1;
    }
    *wrapped = NULL;
    *wrapped_len = 0;

    gss_ctx_id_t ctx = find_gss_ctx(
        tbl, ctx_handle, ctx_handle_len);
    if (ctx == GSS_C_NO_CONTEXT) {
        return -1;
    }

    gss_buffer_desc in_buf = {
        .length = plaintext_len,
        .value  = (void *)plaintext
    };
    gss_buffer_desc out_buf = GSS_C_EMPTY_BUFFER;

    OM_uint32 major, minor;
    int conf_state = 0;
    major = gss_wrap(&minor, ctx, 1 /* conf_req */,
                     GSS_C_QOP_DEFAULT, &in_buf,
                     &conf_state, &out_buf);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *wrapped = malloc(out_buf.length);
    if (*wrapped == NULL) {
        OM_uint32 rel;
        (void)gss_release_buffer(&rel, &out_buf);
        return -1;
    }
    memcpy(*wrapped, out_buf.value, out_buf.length);
    *wrapped_len = out_buf.length;
    OM_uint32 rel;
    (void)gss_release_buffer(&rel, &out_buf);
    return 0;
}
