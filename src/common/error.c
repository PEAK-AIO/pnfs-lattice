/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * error.c — Error code mapping between MDS, NFS4, and errno.
 */

#include <string.h>
#include <errno.h>

#include "pnfs_mds.h"

/** Map mds_status to a human-readable string. */
const char *mds_status_str(enum mds_status s)
{
    switch (s) {
    case MDS_OK:               return "OK";
    case MDS_ERR_NOMEM:        return "out of memory";
    case MDS_ERR_IO:           return "I/O error";
    case MDS_ERR_NOTFOUND:     return "not found";
    case MDS_ERR_EXISTS:       return "already exists";
    case MDS_ERR_INVAL:        return "invalid argument";
    case MDS_ERR_PERM:         return "permission denied";
    case MDS_ERR_NOSPC:        return "no space left";
    case MDS_ERR_NOTEMPTY:     return "directory not empty";
    case MDS_ERR_STALE:        return "stale file handle";
    case MDS_ERR_MOVED:        return "entry moved (referral)";
    case MDS_ERR_GRACE:        return "server in grace period";
    case MDS_ERR_LAYOUTUNAVAIL: return "layout unavailable";
    case MDS_ERR_NOSUPPORT:    return "not supported";
    default:                   return "unknown error";
    }
}

/*
 * mds_status_to_nfs4() is now in compound.c with the proper
 * enum nfs4_status return type.
 */

/** Map errno to mds_status. */
enum mds_status mds_errno_to_status(int err)
{
    switch (err) {
    case 0:       return MDS_OK;
    case ENOMEM:  return MDS_ERR_NOMEM;
    case EIO:     return MDS_ERR_IO;
    case ENOENT:  return MDS_ERR_NOTFOUND;
    case EEXIST:  return MDS_ERR_EXISTS;
    case EINVAL:  return MDS_ERR_INVAL;
    case EACCES:
    case EPERM:   return MDS_ERR_PERM;
    case ENOSPC:  return MDS_ERR_NOSPC;
    case ENOTEMPTY: return MDS_ERR_NOTEMPTY;
    default:      return MDS_ERR_IO;
    }
}
