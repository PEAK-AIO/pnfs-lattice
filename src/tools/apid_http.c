/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * apid_http.c -- Request parsing, authentication, and query mapping for
 * the mds-apid service.
 *
 * Everything here is pure: no sockets, no catalogue, no global state.
 * That keeps the parts an attacker can reach directly -- the token
 * comparison and the request parser -- straightforward to test.
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>

#include "apid_http.h"

bool apid_secret_equal(const char *a, const char *b)
{
    size_t la;
    size_t lb;
    unsigned char diff;
    size_t i;

    if (a == NULL || b == NULL) {
        return false;
    }
    la = strlen(a);
    lb = strlen(b);
    diff = (unsigned char)((la != lb) ? 1U : 0U);

    /* Fixed iteration count: the comparison cost must not reveal where
     * the first mismatch is, nor how long the expected secret is. */
    for (i = 0U; i < APID_TOKEN_MAX; i++) {
        unsigned char ca = (i < la) ? (unsigned char)a[i] : 0U;
        unsigned char cb = (i < lb) ? (unsigned char)b[i] : 0U;

        diff |= (unsigned char)(ca ^ cb);
    }
    return diff == 0U;
}

bool apid_extract_bearer(const char *headers, char *out, size_t cap)
{
    static const char want[] = "\r\nauthorization:";
    const char *p;
    size_t n = 0U;

    if (headers == NULL || out == NULL || cap == 0U) {
        return false;
    }
    out[0] = '\0';

    for (p = headers; *p != '\0'; p++) {
        size_t i = 0U;

        while (want[i] != '\0' &&
               (char)tolower((unsigned char)p[i]) == want[i]) {
            i++;
        }
        if (want[i] == '\0') {
            p += i;
            break;
        }
    }
    if (*p == '\0') {
        return false;
    }

    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (strncasecmp(p, "Bearer", 6) != 0) {
        return false;
    }
    p += 6;
    /* RFC 7235 requires at least one space between scheme and token. */
    if (*p != ' ' && *p != '\t') {
        return false;
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }

    while (p[n] != '\0' && p[n] != '\r' && p[n] != '\n' && n < cap - 1U) {
        out[n] = p[n];
        n++;
    }
    out[n] = '\0';

    /* Trailing optional whitespace is not part of the credential. */
    while (n > 0U && (out[n - 1U] == ' ' || out[n - 1U] == '\t')) {
        out[--n] = '\0';
    }
    return n > 0U;
}

/** Decode one hex digit, or -1 when not hex. */
static int hexval(char c)
{
    if (c >= '0' && c <= '9') { return c - '0'; }
    if (c >= 'a' && c <= 'f') { return c - 'a' + 10; }
    if (c >= 'A' && c <= 'F') { return c - 'A' + 10; }
    return -1;
}

bool apid_url_decode(char *s)
{
    char *w;
    const char *r;

    if (s == NULL) {
        return false;
    }
    w = s;
    r = s;

    while (*r != '\0') {
        if (*r == '%') {
            int hi;
            int lo;

            /* Check the first digit before reading the second, so a
             * trailing "%" cannot read past the terminator. */
            hi = hexval(r[1]);
            if (hi < 0) {
                return false;
            }
            lo = hexval(r[2]);
            if (lo < 0) {
                return false;
            }
            /* Reject an encoded NUL: it would silently truncate the
             * value and could smuggle past later string comparisons. */
            if (hi == 0 && lo == 0) {
                return false;
            }
            *w++ = (char)((hi << 4) | lo);
            r += 3;
        } else if (*r == '+') {
            *w++ = ' ';
            r++;
        } else {
            *w++ = *r++;
        }
    }
    *w = '\0';
    return true;
}

enum apid_request_status apid_split_request_line(char *line, char **path,
                                                 char **query)
{
    char *sp;
    char *qs;

    if (line == NULL || path == NULL || query == NULL) {
        return APID_REQ_MALFORMED;
    }
    *path = NULL;
    *query = NULL;

    if (strncmp(line, "GET ", 4) != 0) {
        /* Anything else, including a well-formed POST, is not served. */
        return APID_REQ_BAD_METHOD;
    }

    *path = line + 4;
    if ((*path)[0] != '/') {
        return APID_REQ_MALFORMED;
    }

    sp = strchr(*path, ' ');
    if (sp != NULL) {
        *sp = '\0';         /* discard the HTTP version */
    }

    qs = strchr(*path, '?');
    if (qs != NULL) {
        *qs = '\0';
        *query = qs + 1;
    }
    return APID_REQ_OK;
}

/** Parse an epoch-seconds parameter into a signed value. */
static bool parse_epoch(const char *v, int64_t *out)
{
    uint64_t u = 0U;

    if (!find_parse_u64(v, &u) || u > (uint64_t)INT64_MAX) {
        return false;
    }
    *out = (int64_t)u;
    return true;
}

/* Parse @v as an unsigned bound into *field and raise its presence flag. */
static bool set_u64_param(const char *v, uint64_t *field, bool *flag)
{
    if (!find_parse_u64(v, field)) {
        return false;
    }
    *flag = true;
    return true;
}

/* Parse @v as epoch seconds into *field and raise its presence flag. */
static bool set_epoch_param(const char *v, int64_t *field, bool *flag)
{
    if (!parse_epoch(v, field)) {
        return false;
    }
    *flag = true;
    return true;
}

static bool set_type_param(struct find_filter *f, const char *v)
{
    int t = find_parse_type_char(v);

    if (t < 0) {
        return false;
    }
    f->type = (uint8_t)t;
    return true;
}

static bool set_name_param(struct find_filter *f, const char *v)
{
    if (v[0] == '\0') {
        return false;
    }
    f->name_glob = v;               /* borrowed from the query string */
    return true;
}

/* inum selects an exact fileid unless inum_max (either order) widens it. */
static bool set_inum_param(struct find_filter *f, const char *v)
{
    if (!find_parse_u64(v, &f->inum_min)) {
        return false;
    }
    if (!f->has_inum) {
        f->inum_max = f->inum_min;  /* exact match unless inum_max follows */
    }
    f->has_inum = true;
    return true;
}

static bool set_limit_param(struct find_filter *f, const char *v)
{
    uint64_t u = 0U;

    if (!find_parse_u64(v, &u) || u == 0U ||
        u > (uint64_t)FIND_LIMIT_MAX) {
        return false;
    }
    f->limit = (uint32_t)u;
    return true;
}

/**
 * Apply one recognised parameter to the filter.
 *
 * @return true when the key is known and the value is valid.
 */
static bool apply_param(struct find_filter *f, const char *k, const char *v)
{
    if (strcmp(k, "type") == 0) {
        return set_type_param(f, v);
    }
    if (strcmp(k, "name") == 0) {
        return set_name_param(f, v);
    }
    if (strcmp(k, "size_gt") == 0) {
        return set_u64_param(v, &f->size_min, &f->has_size_min);
    }
    if (strcmp(k, "size_lt") == 0) {
        return set_u64_param(v, &f->size_max, &f->has_size_max);
    }
    if (strcmp(k, "mtime_after") == 0) {
        return set_epoch_param(v, &f->mtime_min, &f->has_mtime_min);
    }
    if (strcmp(k, "mtime_before") == 0) {
        return set_epoch_param(v, &f->mtime_max, &f->has_mtime_max);
    }
    if (strcmp(k, "ctime_after") == 0) {
        return set_epoch_param(v, &f->ctime_min, &f->has_ctime_min);
    }
    if (strcmp(k, "ctime_before") == 0) {
        return set_epoch_param(v, &f->ctime_max, &f->has_ctime_max);
    }
    if (strcmp(k, "uid") == 0) {
        return set_u64_param(v, &f->uid, &f->has_uid);
    }
    if (strcmp(k, "gid") == 0) {
        return set_u64_param(v, &f->gid, &f->has_gid);
    }
    if (strcmp(k, "inum") == 0) {
        return set_inum_param(f, v);
    }
    if (strcmp(k, "inum_max") == 0) {
        return set_u64_param(v, &f->inum_max, &f->has_inum);
    }
    if (strcmp(k, "limit") == 0) {
        return set_limit_param(f, v);
    }
    return false;
}

bool apid_parse_query(char *qs, struct find_filter *f, const char **why)
{
    const char *ignored = NULL;
    char *save = NULL;
    char *tok;

    if (why == NULL) {
        why = &ignored;
    }
    if (f == NULL) {
        *why = "internal error";
        return false;
    }
    if (qs == NULL || qs[0] == '\0') {
        return true;                    /* no predicates: match anything */
    }

    for (tok = strtok_r(qs, "&", &save); tok != NULL;
         tok = strtok_r(NULL, "&", &save)) {
        char *eq = strchr(tok, '=');

        if (eq == NULL) {
            *why = "malformed parameter, expected key=value";
            return false;
        }
        *eq = '\0';
        if (!apid_url_decode(tok) || !apid_url_decode(eq + 1)) {
            *why = "malformed percent-encoding";
            return false;
        }
        if (!apply_param(f, tok, eq + 1)) {
            *why = "unknown or invalid parameter";
            return false;
        }
    }

    /* inum and inum_max may arrive in either order, so the range can
     * only be validated once the whole query has been consumed. */
    if (f->has_inum && f->inum_min > f->inum_max) {
        *why = "inum range is inverted";
        return false;
    }
    return true;
}

bool apid_bind_allowed(const char *bind_addr, bool tls_enabled,
                       bool insecure)
{
    struct in_addr in;

    if (bind_addr == NULL) {
        return false;
    }
    if (tls_enabled || insecure) {
        return true;
    }
    if (inet_pton(AF_INET, bind_addr, &in) != 1) {
        return false;               /* not a valid literal: refuse */
    }
    return (ntohl(in.s_addr) >> 24) == 127U;
}
