/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_nfs_rpc.c — Minimal NFS3 RPC client for DS file handle capture.
 *
 * Performs MOUNT3 + NFS3 LOOKUP (or CREATE) over a single TCP
 * connection to obtain the raw server file handle for a DS data
 * file.  This FH is embedded in pNFS flex-file layouts for direct
 * client-to-DS I/O.
 *
 * Wire protocol references:
 *   MOUNT v3: RFC 1813 Appendix I (prog 100005)
 *   NFS v3:   RFC 1813 (prog 100003)
 *   RPC:      RFC 5531 (record marking, AUTH_NONE)
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <pthread.h>

#include "ds_nfs_rpc.h"

/* -----------------------------------------------------------------------
 * RPC / NFS constants
 * ----------------------------------------------------------------------- */

#define RPC_CALL          0
#define RPC_REPLY         1
#define RPC_MSG_VER       2
#define AUTH_NONE         0
#define AUTH_UNIX         1
#define MSG_ACCEPTED      0
#define ACCEPT_SUCCESS    0

#define MOUNT_PROGRAM     100005
#define MOUNT_V3          3
#define MOUNTPROC3_MNT    1
#define MNT3_OK           0

#define NFS_PROGRAM       100003
#define NFS_V3            3
#define NFSPROC3_LOOKUP   3
#define NFSPROC3_CREATE   8
#define NFSPROC3_MKDIR    9

#define NFS3_OK           0
#define NFS3ERR_NOENT     2

#define NFS3_FHSIZE       64   /* max v3 FH per RFC 1813 */
#define RPC_BUF_SIZE      2048

#define PMAP_PROGRAM      100000
#define PMAP_V2           2
#define PMAPPROC_GETPORT  3
#define PMAP_PORT         111

static _Atomic uint32_t g_xid = 1000;

/* -----------------------------------------------------------------------
 * Per-DS FH cache — caches the "data/" directory FH so that
 * subsequent LAYOUTGET calls only need a single LOOKUP RPC
 * instead of portmapper + MOUNT + LOOKUP("data") + LOOKUP(file).
 * ----------------------------------------------------------------------- */

#define DS_FH_CACHE_MAX 16

struct ds_fh_cache_entry {
    char     host[64];
    char     export[128];
    uint16_t nfs_port;
    uint8_t  data_dir_fh[NFS3_FHSIZE];
    uint32_t data_dir_fh_len;
    int      valid;
};

static struct ds_fh_cache_entry g_fh_cache[DS_FH_CACHE_MAX];
static pthread_mutex_t g_fh_cache_lock = PTHREAD_MUTEX_INITIALIZER;

static struct ds_fh_cache_entry *fh_cache_find(const char *host,
                                                const char *export)
{
    for (int i = 0; i < DS_FH_CACHE_MAX; i++) {
        if (g_fh_cache[i].valid &&
            strcmp(g_fh_cache[i].host, host) == 0 &&
            strcmp(g_fh_cache[i].export, export) == 0) {
            return &g_fh_cache[i];
        }
    }
    return NULL;
}

static struct ds_fh_cache_entry *fh_cache_alloc(const char *host,
                                                 const char *export)
{
    /* Find free slot or evict slot 0. */
    for (int i = 0; i < DS_FH_CACHE_MAX; i++) {
        if (!g_fh_cache[i].valid) {
            (void)snprintf(g_fh_cache[i].host,
                     sizeof(g_fh_cache[i].host), "%s", host);
            (void)snprintf(g_fh_cache[i].export,
                     sizeof(g_fh_cache[i].export), "%s", export);
            return &g_fh_cache[i];
        }
    }
    /* Evict first entry. */
    g_fh_cache[0].valid = 0;
    (void)snprintf(g_fh_cache[0].host,
             sizeof(g_fh_cache[0].host), "%s", host);
    (void)snprintf(g_fh_cache[0].export,
             sizeof(g_fh_cache[0].export), "%s", export);
    return &g_fh_cache[0];
}

/* -----------------------------------------------------------------------
 * TCP helpers (from ds_probe_null pattern)
 * ----------------------------------------------------------------------- */

static int tcp_connect(const char *host, uint16_t port, uint32_t timeout_ms)
{
    struct addrinfo hints, *res = NULL;
    char port_str[8];
    int fd = -1;

    (void)snprintf(port_str, sizeof(port_str), "%u", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port_str, &hints, &res) != 0) {
        return -1;
    }

    fd = socket(res->ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        freeaddrinfo(res);
        return -1;
    }

    {
        int val = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    }

    /* Non-blocking connect with timeout. */
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
    }

    {
        int crc = connect(fd, res->ai_addr, res->ai_addrlen);
        if (crc != 0 && errno != EINPROGRESS) {
            close(fd);
            freeaddrinfo(res);
            return -1;
        }
        if (crc != 0) {
            struct pollfd pfd = { .fd = fd, .events = POLLOUT };
            if (poll(&pfd, 1, (int)timeout_ms) <= 0) {
                close(fd);
                freeaddrinfo(res);
                return -1;
            }
            int so_err = 0;
            socklen_t sl = sizeof(so_err);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &sl) != 0 ||
                so_err != 0) {
                close(fd);
                freeaddrinfo(res);
                return -1;
            }
        }
    }
    freeaddrinfo(res);

    /* Restore blocking + set timeouts. */
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
        }
        struct timeval tv;
        tv.tv_sec = (time_t)(timeout_ms / 1000U);
        tv.tv_usec = (__suseconds_t)(timeout_ms % 1000U) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }

    return fd;
}

/* -----------------------------------------------------------------------
 * RPC message helpers
 * ----------------------------------------------------------------------- */

/** XDR-encode a uint32 in network byte order. */
static void put_u32(uint8_t *p, uint32_t v)
{
    uint32_t nv = htonl(v);
    memcpy(p, &nv, 4);
}

/** XDR-decode a uint32 from network byte order. */
static uint32_t get_u32(const uint8_t *p)
{
    uint32_t nv;
    memcpy(&nv, p, 4);
    return ntohl(nv);
}

/** XDR-encode an opaque<> (length-prefixed). */
static size_t put_opaque(uint8_t *p, const void *data, uint32_t len)
{
    uint32_t padded = (len + 3) & ~3U;
    put_u32(p, len);
    memcpy(p + 4, data, len);
    if (padded > len) {
        memset(p + 4 + len, 0, padded - len);
    }
    return 4 + padded;
}

/** XDR-encode an NFS3 file handle (opaque<NFS3_FHSIZE>). */
static size_t put_fh3(uint8_t *p, const uint8_t *fh, uint32_t fh_len)
{
    return put_opaque(p, fh, fh_len);
}

/** XDR-encode a string (length-prefixed). */
static size_t put_string(uint8_t *p, const char *s)
{
    uint32_t len = (uint32_t)strlen(s);
    return put_opaque(p, s, len);
}

/**
 * Build an RPC CALL header at @p.
 * Returns the number of bytes written (40).
 */
/**
 * Build an RPC CALL header with AUTH_UNIX (uid=0, gid=0).
 *
 * AUTH_UNIX credential body:
 *   stamp(4) + machinename string(4+0+pad) + uid(4) + gid(4) + gids count(4)
 * = 20 bytes.
 *
 * Total header: 24 (fixed) + 4 (cred_flavor) + 4 (cred_len) + 20 (cred_body)
 *             + 4 (verf_flavor) + 4 (verf_len) = 60 bytes.
 */
static size_t build_rpc_header(uint8_t *p, uint32_t xid,
                               uint32_t prog, uint32_t vers, uint32_t proc)
{
    size_t off = 0;
    put_u32(p + off, xid);          off += 4;
    put_u32(p + off, RPC_CALL);     off += 4;
    put_u32(p + off, RPC_MSG_VER);  off += 4;
    put_u32(p + off, prog);         off += 4;
    put_u32(p + off, vers);         off += 4;
    put_u32(p + off, proc);         off += 4;
    /* AUTH_UNIX credential */
    put_u32(p + off, AUTH_UNIX);    off += 4; /* cred flavor */
    put_u32(p + off, 20);           off += 4; /* cred body length */
    put_u32(p + off, 0);            off += 4; /* stamp */
    put_u32(p + off, 0);            off += 4; /* machinename len=0 */
    put_u32(p + off, 0);            off += 4; /* uid = 0 (root) */
    put_u32(p + off, 0);            off += 4; /* gid = 0 (root) */
    put_u32(p + off, 0);            off += 4; /* aux gids count = 0 */
    /* AUTH_NONE verifier */
    put_u32(p + off, AUTH_NONE);    off += 4;
    put_u32(p + off, 0);            off += 4;
    return off;
}

/**
 * Send a record-marked RPC message and read the reply.
 * Returns bytes in reply_buf (excluding the 4-byte fragment header),
 * or -1 on error.
 */
static int rpc_exchange(int fd, const uint8_t *msg, uint32_t msg_len,
                        uint8_t *reply_buf, uint32_t reply_cap)
{
    /* Send: 4-byte record mark + message. */
    uint32_t frag = htonl(msg_len | 0x80000000U);
    struct iovec iov[2];
    struct msghdr mh;

    memset(&mh, 0, sizeof(mh));
    iov[0].iov_base = &frag;
    iov[0].iov_len = 4;
    iov[1].iov_base = (void *)msg;
    iov[1].iov_len = msg_len;
    mh.msg_iov = iov;
    mh.msg_iovlen = 2;

    ssize_t sent = sendmsg(fd, &mh, MSG_NOSIGNAL);
    if (sent < 0 ||
        (size_t)sent != (size_t)msg_len + 4U) {
        return -1;
    }

    /* Read reply: 4-byte record mark + body. */
    uint8_t rm[4];
    ssize_t nr = recv(fd, rm, 4, MSG_WAITALL);
    if (nr != 4) {
        return -1;
    }

    uint32_t reply_len = ntohl(*(uint32_t *)rm) & 0x7FFFFFFFU;
    if (reply_len > reply_cap) {
        return -1;
    }

    nr = recv(fd, reply_buf, reply_len, MSG_WAITALL);
    if (nr != (ssize_t)reply_len) {
        return -1;
    }

    return (int)reply_len;
}

/**
 * Validate RPC reply header.  Returns pointer past the verifier
 * (start of procedure-specific results), or NULL on error.
 */
static const uint8_t *validate_rpc_reply(const uint8_t *buf, int len,
                                         uint32_t expected_xid)
{
    if (len < 24) {
        return NULL;
    }
    if (get_u32(buf + 0) != expected_xid) {
        return NULL;
    }
    if (get_u32(buf + 4) != RPC_REPLY) {
        return NULL;
    }
    if (get_u32(buf + 8) != MSG_ACCEPTED) {
        return NULL;
    }
    /* Skip verifier: flavor(4) + len(4) + body(len). */
    uint32_t verf_len = get_u32(buf + 16);
    uint32_t off = 20 + verf_len;
    if ((int)off + 4 > len) {
        return NULL;
    }
    if (get_u32(buf + off) != ACCEPT_SUCCESS) {
        return NULL;
    }
    return buf + off + 4;
}

/* -----------------------------------------------------------------------
 * Portmapper — resolve mountd TCP port
 * ----------------------------------------------------------------------- */

/**
 * Query the portmapper (rpcbind) at port 111 to find the TCP port
 * for MOUNT v3 on the given host.
 * @return port > 0 on success, 0 on error.
 */
static uint16_t pmap_getport_mount(const char *host, uint32_t timeout_ms)
{
    int fd = tcp_connect(host, PMAP_PORT, timeout_ms);
    if (fd < 0) {
        return 0;
    }

    uint8_t msg[RPC_BUF_SIZE];
    uint8_t reply[RPC_BUF_SIZE];
    uint32_t xid = atomic_fetch_add(&g_xid, 1);
    size_t off;

    off = build_rpc_header(msg, xid, PMAP_PROGRAM, PMAP_V2, PMAPPROC_GETPORT);
    /* mapping: prog(4) + vers(4) + prot(4) + port(4) */
    put_u32(msg + off, MOUNT_PROGRAM); off += 4;
    put_u32(msg + off, MOUNT_V3);      off += 4;
    put_u32(msg + off, 6);             off += 4; /* IPPROTO_TCP */
    put_u32(msg + off, 0);             off += 4; /* port (ignored) */

    int rlen = rpc_exchange(fd, msg, (uint32_t)off, reply, sizeof(reply));
    close(fd);

    if (rlen < 0) {
        return 0;
    }

    const uint8_t *p = validate_rpc_reply(reply, rlen, xid);
    if (p == NULL) {
        return 0;
    }

    uint32_t port = get_u32(p);
    return (port > 0 && port <= 65535) ? (uint16_t)port : 0;
}

/* -----------------------------------------------------------------------
 * MOUNT3 MNT — obtain root file handle for an export
 * ----------------------------------------------------------------------- */

/**
 * Perform MOUNT3 MNT to get the root FH for @a export_path.
 * @return 0 on success, -1 on error.
 */
static int mount3_mnt(int fd, const char *export_path,
                      uint8_t *root_fh, uint32_t *root_fh_len)
{
    uint8_t msg[RPC_BUF_SIZE];
    uint8_t reply[RPC_BUF_SIZE];
    uint32_t xid = atomic_fetch_add(&g_xid, 1);
    size_t off;

    off = build_rpc_header(msg, xid, MOUNT_PROGRAM, MOUNT_V3, MOUNTPROC3_MNT);
    off += put_string(msg + off, export_path);

    int rlen = rpc_exchange(fd, msg, (uint32_t)off, reply, sizeof(reply));
    if (rlen < 0) {
        return -1;
    }

    const uint8_t *p = validate_rpc_reply(reply, rlen, xid);
    if (p == NULL) {
        return -1;
    }

    /* mountres3: status(4) + fhandle3 (opaque<NFS3_FHSIZE>). */
    if (get_u32(p) != MNT3_OK) {
        return -1;
    }
    p += 4;

    uint32_t fh_len = get_u32(p);
    p += 4;
    if (fh_len > NFS3_FHSIZE || fh_len > *root_fh_len) {
        return -1;
    }
    memcpy(root_fh, p, fh_len);
    *root_fh_len = fh_len;
    return 0;
}

/* -----------------------------------------------------------------------
 * NFS3 LOOKUP — resolve a single path component
 * ----------------------------------------------------------------------- */

/**
 * NFS3 LOOKUP: resolve @a name within directory @a dir_fh.
 * On success, writes the object FH to @a out_fh / @a out_fh_len.
 * @return NFS3 status (0 = OK, 2 = NOENT, etc.).
 */
static int nfs3_lookup(int fd,
                       const uint8_t *dir_fh, uint32_t dir_fh_len,
                       const char *name,
                       uint8_t *out_fh, uint32_t *out_fh_len)
{
    uint8_t msg[RPC_BUF_SIZE];
    uint8_t reply[RPC_BUF_SIZE];
    uint32_t xid = atomic_fetch_add(&g_xid, 1);
    size_t off;

    off = build_rpc_header(msg, xid, NFS_PROGRAM, NFS_V3, NFSPROC3_LOOKUP);
    /* diropargs3 = fhandle3 + filename3 */
    off += put_fh3(msg + off, dir_fh, dir_fh_len);
    off += put_string(msg + off, name);

    int rlen = rpc_exchange(fd, msg, (uint32_t)off, reply, sizeof(reply));
    if (rlen < 0) {
        return -1;
    }

    const uint8_t *p = validate_rpc_reply(reply, rlen, xid);
    if (p == NULL) {
        return -1;
    }

    uint32_t status = get_u32(p);
    p += 4;

    if (status != NFS3_OK) {
        return (int)status;
    }

    /* LOOKUP3resok: object FH (opaque<NFS3_FHSIZE>) + post_op_attr + dir_attr. */
    uint32_t fh_len = get_u32(p);
    p += 4;
    if (fh_len > NFS3_FHSIZE || fh_len > *out_fh_len) {
        return -1;
    }
    memcpy(out_fh, p, fh_len);
    *out_fh_len = fh_len;
    return NFS3_OK;
}

/* -----------------------------------------------------------------------
 * NFS3 MKDIR — create a directory
 * ----------------------------------------------------------------------- */

/**
 * NFS3 MKDIR: create directory @a name in @a dir_fh.
 * @return NFS3 status.
 */
static int nfs3_mkdir(int fd,
                      const uint8_t *dir_fh, uint32_t dir_fh_len,
                      const char *name,
                      uint8_t *out_fh, uint32_t *out_fh_len)
{
    uint8_t msg[RPC_BUF_SIZE];
    uint8_t reply[RPC_BUF_SIZE];
    uint32_t xid = atomic_fetch_add(&g_xid, 1);
    size_t off;

    off = build_rpc_header(msg, xid, NFS_PROGRAM, NFS_V3, NFSPROC3_MKDIR);
    /* diropargs3 = fhandle3 + filename3 */
    off += put_fh3(msg + off, dir_fh, dir_fh_len);
    off += put_string(msg + off, name);
    /* sattr3: mode=0755, all others "don't set" */
    put_u32(msg + off, 1); off += 4; /* set_mode = TRUE */
    put_u32(msg + off, 0755); off += 4;
    put_u32(msg + off, 0); off += 4; /* set_uid = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_gid = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_size = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_atime = DONT_CHANGE */
    put_u32(msg + off, 0); off += 4; /* set_mtime = DONT_CHANGE */

    int rlen = rpc_exchange(fd, msg, (uint32_t)off, reply, sizeof(reply));
    if (rlen < 0) {
        return -1;
    }

    const uint8_t *p = validate_rpc_reply(reply, rlen, xid);
    if (p == NULL) {
        return -1;
    }

    uint32_t status = get_u32(p);
    p += 4;

    if (status != NFS3_OK) {
        return (int)status;
    }

    /* MKDIR3resok: post_op_fh3 (bool + fhandle3) + obj_attrs + dir_wcc. */
    uint32_t has_fh = get_u32(p);
    p += 4;
    if (!has_fh) {
        return -1;
    }
    uint32_t fh_len = get_u32(p);
    p += 4;
    if (fh_len > NFS3_FHSIZE || fh_len > *out_fh_len) {
        return -1;
    }
    memcpy(out_fh, p, fh_len);
    *out_fh_len = fh_len;
    return NFS3_OK;
}

/* -----------------------------------------------------------------------
 * NFS3 CREATE — create a regular file
 * ----------------------------------------------------------------------- */

/**
 * NFS3 CREATE: create file @a name in @a dir_fh.
 * Uses UNCHECKED mode (create if absent, no-op if exists).
 * @return NFS3 status.
 */
static int nfs3_create(int fd,
                       const uint8_t *dir_fh, uint32_t dir_fh_len,
                       const char *name,
                       uint8_t *out_fh, uint32_t *out_fh_len)
{
    uint8_t msg[RPC_BUF_SIZE];
    uint8_t reply[RPC_BUF_SIZE];
    uint32_t xid = atomic_fetch_add(&g_xid, 1);
    size_t off;

    off = build_rpc_header(msg, xid, NFS_PROGRAM, NFS_V3, NFSPROC3_CREATE);
    /* diropargs3 */
    off += put_fh3(msg + off, dir_fh, dir_fh_len);
    off += put_string(msg + off, name);
    /* createhow3: UNCHECKED (0) + sattr3 */
    put_u32(msg + off, 0); off += 4; /* mode = UNCHECKED */
    /* sattr3: mode=0644, rest "don't set" */
    put_u32(msg + off, 1); off += 4; /* set_mode = TRUE */
    put_u32(msg + off, 0644); off += 4;
    put_u32(msg + off, 0); off += 4; /* set_uid = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_gid = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_size = FALSE */
    put_u32(msg + off, 0); off += 4; /* set_atime = DONT_CHANGE */
    put_u32(msg + off, 0); off += 4; /* set_mtime = DONT_CHANGE */

    int rlen = rpc_exchange(fd, msg, (uint32_t)off, reply, sizeof(reply));
    if (rlen < 0) {
        return -1;
    }

    const uint8_t *p = validate_rpc_reply(reply, rlen, xid);
    if (p == NULL) {
        return -1;
    }

    uint32_t status = get_u32(p);
    p += 4;

    if (status != NFS3_OK) {
        return (int)status;
    }

    /* CREATE3resok: post_op_fh3 (bool + fhandle3) + obj_attrs + dir_wcc. */
    uint32_t has_fh = get_u32(p);
    p += 4;
    if (!has_fh) {
        return -1;
    }
    uint32_t fh_len = get_u32(p);
    p += 4;
    if (fh_len > NFS3_FHSIZE || fh_len > *out_fh_len) {
        return -1;
    }
    memcpy(out_fh, p, fh_len);
    *out_fh_len = fh_len;
    return NFS3_OK;
}

/* -----------------------------------------------------------------------
 * Public API: ds_nfs3_lookup_fh
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int ds_nfs3_lookup_fh(const char *host, uint16_t port,
                      const char *export_path,
                      const char *rel_path,
                      int create,
                      uint8_t *fh_out, uint32_t *fh_len,
                      uint32_t timeout_ms)
{
    sigset_t mask, old_mask;
    int fd = -1;
    int rc = -1;
    uint8_t cur_fh[NFS3_FHSIZE];
    uint32_t cur_fh_len = NFS3_FHSIZE;

    if (host == NULL || export_path == NULL || rel_path == NULL ||
        fh_out == NULL || fh_len == NULL || *fh_len == 0) {
        return -1;
    }

    /* Block signals to prevent NDB EINTR interference. */
    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, &old_mask);

    /* Fast path: check if we have the "data/" directory FH cached.
     * This avoids portmapper + MOUNT + LOOKUP("data") on every call,
     * reducing 3 TCP connections to 1 per LAYOUTGET. */
    {
        const struct ds_fh_cache_entry *ce;
        pthread_mutex_lock(&g_fh_cache_lock);
        ce = fh_cache_find(host, export_path);
        if (ce != NULL && ce->data_dir_fh_len > 0) {
            memcpy(cur_fh, ce->data_dir_fh, ce->data_dir_fh_len);
            cur_fh_len = ce->data_dir_fh_len;
            pthread_mutex_unlock(&g_fh_cache_lock);

            /* Single TCP connection: LOOKUP the file name. */
            fd = tcp_connect(host, port, timeout_ms);
            if (fd < 0) { goto out; }

            /* Extract just the filename (last component of rel_path). */
            const char *fname = strrchr(rel_path, '/');
            fname = fname ? fname + 1 : rel_path;

            uint8_t file_fh[NFS3_FHSIZE];
            uint32_t file_fh_len = NFS3_FHSIZE;
            int st = nfs3_lookup(fd, cur_fh, cur_fh_len,
                                 fname, file_fh, &file_fh_len);
            if (st == NFS3ERR_NOENT && create) {
                file_fh_len = NFS3_FHSIZE;
                st = nfs3_create(fd, cur_fh, cur_fh_len,
                                 fname, file_fh, &file_fh_len);
            }
            if (st == NFS3_OK && file_fh_len <= *fh_len) {
                memcpy(fh_out, file_fh, file_fh_len);
                *fh_len = file_fh_len;
                rc = 0;
            }
            goto out;
        }
        pthread_mutex_unlock(&g_fh_cache_lock);
    }

    /* Slow path: portmapper + MOUNT + LOOKUP("data") + LOOKUP(file).
     * Populates the cache for subsequent fast-path calls. */
    {
        uint16_t mount_port = pmap_getport_mount(host, timeout_ms);
        if (mount_port == 0) {
            goto out;
        }
        int mount_fd = tcp_connect(host, mount_port, timeout_ms);
        if (mount_fd < 0) {
            goto out;
        }
        int mrc = mount3_mnt(mount_fd, export_path, cur_fh, &cur_fh_len);
        close(mount_fd);
        if (mrc != 0) {
            goto out;
        }
    }

    fd = tcp_connect(host, port, timeout_ms);
    if (fd < 0) {
        goto out;
    }

    /* Walk path components, caching the "data/" dir FH. */
    {
        char path_buf[512];
        char *components[16];
        uint32_t ncomp = 0;
        char *p;

        (void)snprintf(path_buf, sizeof(path_buf), "%s", rel_path);

        for (p = path_buf; *p != '\0' && ncomp < 16; ) {
            while (*p == '/') { p++; }
            if (*p == '\0') { break; }
            components[ncomp++] = p;
            while (*p != '/' && *p != '\0') { p++; }
            if (*p == '/') { *p++ = '\0'; }
        }

        for (uint32_t ci = 0; ci < ncomp; ci++) {
            int is_last = (ci == ncomp - 1);
            uint8_t child_fh[NFS3_FHSIZE];
            uint32_t child_fh_len = NFS3_FHSIZE;

            int st = nfs3_lookup(fd, cur_fh, cur_fh_len,
                                 components[ci], child_fh, &child_fh_len);

            if (st == NFS3ERR_NOENT) {
                if (is_last && create) {
                    child_fh_len = NFS3_FHSIZE;
                    st = nfs3_create(fd, cur_fh, cur_fh_len,
                                     components[ci],
                                     child_fh, &child_fh_len);
                } else if (!is_last) {
                    child_fh_len = NFS3_FHSIZE;
                    st = nfs3_mkdir(fd, cur_fh, cur_fh_len,
                                    components[ci],
                                    child_fh, &child_fh_len);
                }
                if (st != NFS3_OK) {
                    goto out;
                }
            } else if (st != NFS3_OK) {
                goto out;
            }

            memcpy(cur_fh, child_fh, child_fh_len);
            cur_fh_len = child_fh_len;

            /* Cache the directory FH (second-to-last component = "data/"). */
            if (!is_last) {
                pthread_mutex_lock(&g_fh_cache_lock);
                struct ds_fh_cache_entry *ce = fh_cache_find(host, export_path);
                if (ce == NULL) {
                    ce = fh_cache_alloc(host, export_path);
                }
                memcpy(ce->data_dir_fh, cur_fh, cur_fh_len);
                ce->data_dir_fh_len = cur_fh_len;
                ce->nfs_port = port;
                ce->valid = 1;
                pthread_mutex_unlock(&g_fh_cache_lock);
            }
        }
    }

    /* Copy final FH to output. */
    if (cur_fh_len > *fh_len) {
        goto out;
    }
    memcpy(fh_out, cur_fh, cur_fh_len);
    *fh_len = cur_fh_len;
    rc = 0;

out:
    if (fd >= 0) {
        close(fd);
    }
    pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
    return rc;
}
