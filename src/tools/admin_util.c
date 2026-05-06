/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * admin_util.c -- Shared helpers for the mds-admin CLI tool.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "admin_util.h"
#include "mds_catalogue.h"

/* ----------------------------------------------------------------------- */

int parse_admin_endpoint(int argc, const char *const *argv,
                         const char **host, uint16_t *port,
                         bool *json)
{
    *host = DEFAULT_MDS_HOST;
    *port = DEFAULT_MDS_PORT;
    if (json != NULL) {
        *json = false;
    }

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            *host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0) {
            if (i + 1 >= argc) {
                return -1;
            }
            char *endptr = NULL;
            unsigned long v = strtoul(argv[++i], &endptr, 10);
            if (endptr == argv[i] || *endptr != '\0' ||
                v == 0 || v > 65535) {
                return -1;
            }
            *port = (uint16_t)v;
        } else if (json != NULL && strcmp(argv[i], "--json") == 0) {
            *json = true;
        }
    }
    return 0;
}

int parse_dest_endpoint(int argc, const char *const *argv,
                        const char **host, uint16_t *port,
                        bool *have_host, bool *have_port)
{
    *host = DEFAULT_DEST_HOST;
    *port = DEFAULT_DEST_PORT;
    *have_host = false;
    *have_port = false;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            *host = argv[++i];
            *have_host = true;
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v == 0 || v > 65535) {
                return -1;
            }
            *port = (uint16_t)v;
            *have_port = true;
        }
    }
    return 0;
}

int json_escape_string(const char *in, char *out, size_t cap)
{
    if (in == NULL || out == NULL || cap == 0) {
        return -1;
    }

    size_t wi = 0;  /* write index */

    for (size_t ri = 0; in[ri] != '\0'; ri++) {
        unsigned char ch = (unsigned char)in[ri];
        const char *esc = NULL;
        char hex_buf[8];

        switch (ch) {
        case '"':  esc = "\\\""; break;
        case '\\': esc = "\\\\"; break;
        case '\b': esc = "\\b";  break;
        case '\f': esc = "\\f";  break;
        case '\n': esc = "\\n";  break;
        case '\r': esc = "\\r";  break;
        case '\t': esc = "\\t";  break;
        default:
            if (ch < 0x20) {
                (void)snprintf(hex_buf, sizeof(hex_buf),
                               "\\u%04x", (unsigned)ch);
                esc = hex_buf;
            }
            break;
        }

        if (esc != NULL) {
            size_t elen = strlen(esc);
            if (wi + elen >= cap) {
                out[wi] = '\0';
                return -1;
            }
            memcpy(out + wi, esc, elen);
            wi += elen;
        } else {
            if (wi + 1 >= cap) {
                out[wi] = '\0';
                return -1;
            }
            out[wi++] = (char)ch;
        }
    }

    out[wi] = '\0';
    return (int)wi;
}

int parse_ds_state(const char *name, uint32_t *out)
{
    if (name == NULL || out == NULL) {
        return -1;
    }
    if (strcmp(name, "online") == 0)      { *out = 0; return 0; }
    if (strcmp(name, "offline") == 0)     { *out = 1; return 0; }
    if (strcmp(name, "draining") == 0)    { *out = 2; return 0; }
    if (strcmp(name, "rebalancing") == 0) { *out = 3; return 0; }
    return -1;
}

int parse_ds_mode(const char *name, uint8_t *out)
{
    if (name == NULL || out == NULL) {
        return -1;
    }
    if (strcmp(name, "generic") == 0) { *out = 1; return 0; }
    return -1;
}

int parse_ds_transport(const char *name, uint8_t *out)
{
    if (name == NULL || out == NULL) {
        return -1;
    }
    if (strcmp(name, "tcp") == 0)      { *out = 0x01; return 0; }
    if (strcmp(name, "rdma") == 0)     { *out = 0x02; return 0; }
    if (strcmp(name, "tcp,rdma") == 0) { *out = 0x03; return 0; }
    if (strcmp(name, "rdma,tcp") == 0) { *out = 0x03; return 0; }
    return -1;
}


/* -----------------------------------------------------------------------
 * DS import/scan helpers
 * ----------------------------------------------------------------------- */

#include <inttypes.h>

int parse_ds_filename(const char *name, uint64_t *fileid,
                      uint32_t *stripe, uint32_t *mirror)
{
    unsigned long long fid;
    unsigned int s, m;

    if (name == NULL || fileid == NULL ||
        stripe == NULL || mirror == NULL) {
        return -1;
    }
    /* NOLINTNEXTLINE(cert-err34-c) */
    if (sscanf(name, "%llu_%u_%u", &fid, &s, &m) != 3) {
        return -1;
    }
    *fileid = (uint64_t)fid;
    *stripe = s;
    *mirror = m;
    return 0;
}

enum mds_status resolve_mds_path(void *unused,
                                 const char *path, const uint64_t *fileid)
{
    (void)unused;
    (void)path;
    (void)fileid;
    /* Path resolution requires catalogue. */
    return MDS_ERR_NOSUPPORT;
}

#include <sys/sendfile.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int copy_file_safe(const char *src, const char *dst)
{
    int sfd = -1, dfd = -1;
    struct stat sb;
    int rc = -1;

    sfd = open(src, O_RDONLY);
    if (sfd < 0) { return -1; }

    if (fstat(sfd, &sb) != 0) { goto out; }

    dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, sb.st_mode & 07777);
    if (dfd < 0) { goto out; }

    {
        off_t off = 0;
        ssize_t total = (ssize_t)sb.st_size;
        while (off < total) {
            ssize_t n = sendfile(dfd, sfd, &off, (size_t)(total - off));
            if (n <= 0) { goto out; }
        }
    }
    rc = 0;
out:
    if (sfd >= 0) { close(sfd); }
    if (dfd >= 0) { close(dfd); }
    return rc;
}
