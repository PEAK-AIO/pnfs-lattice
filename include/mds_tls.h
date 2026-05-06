/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_tls.h -- TLS helper for inter-MDS transport.
 *
 * Wraps OpenSSL SSL_CTX and SSL for cluster transport,
 * replication, and etcd connections.
 */

#ifndef MDS_TLS_H
#define MDS_TLS_H

#include <stdbool.h>
#include <stddef.h>

/* Opaque TLS context. */
struct mds_tls_ctx;

/**
 * Create a TLS context from certificate files.
 *
 * @param ca_file    CA certificate path.
 * @param cert_file  Node certificate path.
 * @param key_file   Node private key path.
 * @param is_server  true for server mode, false for client.
 * @param require_peer  Require peer certificate verification.
 * @param out        Receives context handle.
 * @return 0 on success, -1 on error.
 */
int mds_tls_ctx_create(const char *ca_file,
                       const char *cert_file,
                       const char *key_file,
                       bool is_server,
                       bool require_peer,
                       struct mds_tls_ctx **out);

/** Destroy a TLS context. */
void mds_tls_ctx_destroy(struct mds_tls_ctx *ctx);

/* Opaque TLS connection. */
struct mds_tls_conn;

/**
 * Wrap an existing TCP fd with TLS.
 *
 * @param ctx        TLS context.
 * @param fd         Connected TCP socket.
 * @param is_server  true = accept, false = connect.
 * @param out        Receives connection handle.
 * @return 0 on success, -1 on handshake failure.
 */
int mds_tls_wrap(struct mds_tls_ctx *ctx, int fd,
                 bool is_server,
                 const char *expected_hostname,
                 struct mds_tls_conn **out);

/** Read from TLS connection. */
int mds_tls_read(struct mds_tls_conn *conn, void *buf, size_t len);

/** Write to TLS connection. */
int mds_tls_write(struct mds_tls_conn *conn,
                  const void *buf, size_t len);

/** Close and free TLS connection (does NOT close the underlying fd). */
void mds_tls_close(struct mds_tls_conn *conn);

/* --- Additions for live TLS transport (Item 49 Stage 3) --- */

/**
 * Return the number of bytes buffered inside the TLS layer that
 * can be read immediately without hitting the network.
 *
 * Useful before poll(): if pending > 0 the next mds_tls_read will
 * not block even if poll(fd) says no data.
 *
 * @return Byte count (0 when conn is NULL or nothing buffered).
 */
size_t mds_tls_pending(const struct mds_tls_conn *conn);

/**
 * Peek at up to @len bytes without consuming them.
 *
 * @return Number of bytes peeked (>0), or -1 on error.
 */
int mds_tls_peek(struct mds_tls_conn *conn, void *buf, size_t len);

#endif /* MDS_TLS_H */
