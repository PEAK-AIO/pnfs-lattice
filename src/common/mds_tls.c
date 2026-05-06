/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_tls.c -- TLS helper using OpenSSL.
 */

#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

#include "mds_tls.h"

struct mds_tls_ctx {
    SSL_CTX *ssl_ctx;
};

struct mds_tls_conn {
    SSL *ssl;
};

int mds_tls_ctx_create(const char *ca_file,
                       const char *cert_file,
                       const char *key_file,
                       bool is_server,
                       bool require_peer,
                       struct mds_tls_ctx **out)
{
    if (out == NULL) {
        return -1;
    }

    const SSL_METHOD *method = is_server
        ? TLS_server_method()
        : TLS_client_method();

    SSL_CTX *ssl_ctx = SSL_CTX_new(method);
    if (ssl_ctx == NULL) {
        return -1;
    }

    /* Minimum TLS 1.2. */
    SSL_CTX_set_min_proto_version(ssl_ctx, TLS1_2_VERSION);

    if (ca_file != NULL && ca_file[0] != '\0') {
        if (SSL_CTX_load_verify_locations(ssl_ctx,
                                           ca_file, NULL) != 1) {
            SSL_CTX_free(ssl_ctx);
            return -1;
        }
    }

    if (cert_file != NULL && cert_file[0] != '\0') {
        if (SSL_CTX_use_certificate_chain_file(ssl_ctx,
                                                cert_file) != 1) {
            SSL_CTX_free(ssl_ctx);
            return -1;
        }
    }

    if (key_file != NULL && key_file[0] != '\0') {
        if (SSL_CTX_use_PrivateKey_file(ssl_ctx, key_file,
                                         SSL_FILETYPE_PEM) != 1) {
            SSL_CTX_free(ssl_ctx);
            return -1;
        }
        if (SSL_CTX_check_private_key(ssl_ctx) != 1) {
            SSL_CTX_free(ssl_ctx);
            return -1;
        }
    }

    if (require_peer) {
        SSL_CTX_set_verify(ssl_ctx,
            SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
            NULL);
    }

    struct mds_tls_ctx *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        SSL_CTX_free(ssl_ctx);
        return -1;
    }
    ctx->ssl_ctx = ssl_ctx;
    *out = ctx;
    return 0;
}

void mds_tls_ctx_destroy(struct mds_tls_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    if (ctx->ssl_ctx != NULL) {
        SSL_CTX_free(ctx->ssl_ctx);
    }
    free(ctx);
}

int mds_tls_wrap(struct mds_tls_ctx *ctx, int fd,
                 bool is_server,
                 const char *expected_hostname,
                 struct mds_tls_conn **out)
{
    if (ctx == NULL || out == NULL || fd < 0) {
        return -1;
    }

    SSL *ssl = SSL_new(ctx->ssl_ctx);
    if (ssl == NULL) {
        return -1;
    }

    if (SSL_set_fd(ssl, fd) != 1) {
        SSL_free(ssl);
        return -1;
    }


    /* Hostname / IP verification for client connections. */
    if (!is_server && expected_hostname != NULL &&
            expected_hostname[0] != '\0') {
        /* Detect IP address vs DNS hostname.  IP addresses
         * need SSL_set1_ip_asc to match IP SANs; DNS names
         * use SSL_set1_host to match DNS SANs. */
        struct in_addr dummy4;
        struct in6_addr dummy6;
        if (inet_pton(AF_INET, expected_hostname, &dummy4) == 1 ||
                inet_pton(AF_INET6, expected_hostname, &dummy6) == 1) {
            X509_VERIFY_PARAM *vpm = SSL_get0_param(ssl);
            if (X509_VERIFY_PARAM_set1_ip_asc(vpm,
                    expected_hostname) != 1) {
                SSL_free(ssl);
                return -1;
            }
        } else {
            SSL_set_hostflags(ssl,
                X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
            if (SSL_set1_host(ssl, expected_hostname) != 1) {
                SSL_free(ssl);
                return -1;
            }
        }
        SSL_set_verify(ssl, SSL_VERIFY_PEER, NULL);
    }

    int rc = is_server ? SSL_accept(ssl) : SSL_connect(ssl);
    if (rc != 1) {
        SSL_free(ssl);
        return -1;
    }

    struct mds_tls_conn *conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
        return -1;
    }
    conn->ssl = ssl;
    *out = conn;
    return 0;
}

int mds_tls_read(struct mds_tls_conn *conn, void *buf, size_t len)
{
    if (conn == NULL || conn->ssl == NULL) {
        return -1;
    }
    int n = SSL_read(conn->ssl, buf, (int)len);
    return (n > 0) ? n : -1;
}

int mds_tls_write(struct mds_tls_conn *conn,
                  const void *buf, size_t len)
{
    if (conn == NULL || conn->ssl == NULL) {
        return -1;
    }
    int n = SSL_write(conn->ssl, buf, (int)len);
    return (n > 0) ? n : -1;
}

void mds_tls_close(struct mds_tls_conn *conn)
{
    if (conn == NULL) {
        return;
    }
    if (conn->ssl != NULL) {
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
    }
    free(conn);
}

/* --- Live TLS transport helpers (Item 49 Stage 3) --- */

size_t mds_tls_pending(const struct mds_tls_conn *conn)
{
    if (conn == NULL || conn->ssl == NULL) {
        return 0;
    }
    int p = SSL_pending(conn->ssl);
    return (p > 0) ? (size_t)p : 0;
}

int mds_tls_peek(struct mds_tls_conn *conn, void *buf, size_t len)
{
    if (conn == NULL || conn->ssl == NULL) {
        return -1;
    }
    int n = SSL_peek(conn->ssl, buf, (int)len);
    return (n > 0) ? n : -1;
}
