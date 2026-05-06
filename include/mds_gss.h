/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_gss.h -- RPCSEC_GSS context management for NFS auth.
 *
 * Handles GSS-API token exchange for Kerberos authentication.
 * Compile-optional: requires libkrb5-dev.
 */

#ifndef MDS_GSS_H
#define MDS_GSS_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* RPCSEC_GSS procedures (RFC 2203). */
#define RPCSEC_GSS_DATA          0
#define RPCSEC_GSS_INIT          1
#define RPCSEC_GSS_CONTINUE_INIT 2
#define RPCSEC_GSS_DESTROY       3

/* RPCSEC_GSS services. */
#define RPCSEC_GSS_SVC_NONE      1  /* krb5: auth only */
#define RPCSEC_GSS_SVC_INTEGRITY 2  /* krb5i: integrity */
#define RPCSEC_GSS_SVC_PRIVACY   3  /* krb5p: privacy */

/* Parsed RPCSEC_GSS credential header. */
struct rpc_gss_cred {
    uint32_t version;
    uint32_t procedure;
    uint32_t seq_num;
    uint32_t service;
    uint32_t ctx_handle_len;
    uint8_t  ctx_handle[64];
    uint32_t verf_flavor;
    uint32_t verf_body_len;
    uint8_t  verf_body[400];
    uint32_t cred_end_pos; /**< XDR pos after credential. */
};

/* GSS context table (opaque). */
struct mds_gss_table;

/**
 * Initialize GSS context table.
 *
 * @param keytab_path  Path to Kerberos keytab (NULL = default).
 * @param principal    Service principal (NULL = default).
 * @param out          Receives table handle.
 * @return 0 on success, -1 on error.
 */
int mds_gss_init(const char *keytab_path, const char *principal,
                 struct mds_gss_table **out);

/** Destroy GSS context table. */
void mds_gss_destroy(struct mds_gss_table *tbl);

/**
 * Process a GSS INIT/CONTINUE token.
 *
 * @param tbl          Context table.
 * @param in_token     Input GSS token from client.
 * @param in_len       Input token length.
 * @param out_token    Receives output token (caller frees).
 * @param out_len      Receives output token length.
 * @param ctx_handle   Receives context handle for client.
 * @param ctx_len      Receives handle length.
 * @param complete     Set to true when context is established.
 * @return 0 on success, -1 on error.
 */
int mds_gss_accept_token(struct mds_gss_table *tbl,
                         uint32_t gss_proc,
                         const uint8_t *existing_handle,
                         uint32_t existing_handle_len,
                         const void *in_token, size_t in_len,
                         void **out_token, size_t *out_len,
                         uint8_t *ctx_handle, uint32_t *ctx_len,
                         bool *complete);

/**
 * Validate a GSS DATA request.
 *
 * @param tbl       Context table.
 * @param cred      Parsed GSS credential.
 * @param service   Receives effective service level.
 * @return 0 if valid, -1 if context unknown or expired.
 */
/**
 * Destroy a single GSS context by handle.
 *
 * @param tbl            Context table.
 * @param ctx_handle     Context handle bytes.
 * @param ctx_handle_len Handle length (must be >= 4).
 * @return 0 on success, -1 if not found.
 */
int mds_gss_destroy_context(struct mds_gss_table *tbl,
                            const uint8_t *ctx_handle,
                            uint32_t ctx_handle_len);

/**
 * Verify the request header verifier (RFC 2203 s5.3.1).
 * The verifier must be gss_get_mic(seq_num).
 *
 * @return 0 on success, -1 on failure.
 */
int mds_gss_verify_header(struct mds_gss_table *tbl,
                          const struct rpc_gss_cred *cred,
                          const void *header_bytes,
                          size_t header_len);

int mds_gss_validate(struct mds_gss_table *tbl,
                     const struct rpc_gss_cred *cred,
                     uint32_t *service);

/**
 * Verify MIC on a message (krb5i request path).
 *
 * @return 0 on success, -1 on failure.
 */
int mds_gss_verify_data(struct mds_gss_table *tbl,
                        const uint8_t *ctx_handle,
                        uint32_t ctx_handle_len,
                        const void *message,
                        size_t message_len,
                        const void *mic_token,
                        size_t mic_token_len);

/**
 * Compute MIC over a message (krb5i reply path).
 * Caller frees *mic_out.
 *
 * @return 0 on success, -1 on failure.
 */
int mds_gss_get_mic(struct mds_gss_table *tbl,
                    const uint8_t *ctx_handle,
                    uint32_t ctx_handle_len,
                    const void *message,
                    size_t message_len,
                    void **mic_out,
                    size_t *mic_out_len);

/**
 * Unwrap (decrypt) a message (krb5p request path).
 * Caller frees *plaintext.
 *
 * @return 0 on success, -1 on failure.
 */
int mds_gss_unwrap_data(struct mds_gss_table *tbl,
                        const uint8_t *ctx_handle,
                        uint32_t ctx_handle_len,
                        const void *wrapped,
                        size_t wrapped_len,
                        void **plaintext,
                        size_t *plaintext_len);

/**
 * Wrap (encrypt) a message (krb5p reply path).
 * Caller frees *wrapped.
 *
 * @return 0 on success, -1 on failure.
 */
int mds_gss_wrap_data(struct mds_gss_table *tbl,
                      const uint8_t *ctx_handle,
                      uint32_t ctx_handle_len,
                      const void *plaintext,
                      size_t plaintext_len,
                      void **wrapped,
                      size_t *wrapped_len);

/**
 * Evict established GSS contexts that have been idle for more than
 * @max_idle_sec seconds.  Call periodically (e.g. every 60s) from
 * the main loop or a timer thread.
 *
 * @param tbl          Context table.
 * @param max_idle_sec Maximum idle time before eviction.
 * @return Number of contexts evicted.
 */
uint32_t mds_gss_evict_idle(struct mds_gss_table *tbl,
                            uint32_t max_idle_sec);

#endif /* MDS_GSS_H */
