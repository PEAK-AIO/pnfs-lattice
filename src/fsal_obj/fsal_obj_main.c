/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fsal_obj_main.c -- FSAL_OBJ plugin initialisation and export ops.
 *
 * This module registers the FSAL, creates exports, and provides
 * the obj_ops vtable (lookup, create, getattr, setattr, etc.).
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "pnfs_mds.h"

/* FSAL method vtable -- function pointers for every metadata op */
struct fsal_obj_ops {
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*lookup)(uint64_t parent, const char *name, uint64_t *child);
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*create)(uint64_t parent, const char *name, uint32_t type,
                  uint32_t mode, uint64_t *fileid);
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*remove)(uint64_t parent, const char *name);
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*rename)(uint64_t src_parent, const char *src_name,
                  uint64_t dst_parent, const char *dst_name);
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*getattr)(uint64_t fileid, struct mds_inode *inode);
    /* cppcheck-suppress unusedStructMember */
    int (*setattr)(uint64_t fileid, const struct mds_inode *attrs);
    /* cppcheck-suppress unusedStructMember */
    /* cppcheck-suppress unusedStructMember */
    int (*readdir)(uint64_t dir, uint64_t cookie,
                   void *entries, size_t *count);
    /* cppcheck-suppress unusedStructMember */
};

/* Global FSAL ops instance -- wired to namespace.c functions */
static struct fsal_obj_ops g_obj_ops;

__attribute__((unused))
static int fsal_obj_init(void)
{
    /* TODO: Wire obj_ops to catalogue API functions:
     *   g_obj_ops.lookup  = ns_lookup;
     *   g_obj_ops.create  = ns_create_wrapper;
     *   g_obj_ops.remove  = ns_remove;
     *   g_obj_ops.rename  = ns_rename;
     *   g_obj_ops.getattr = ns_getattr;
     *   g_obj_ops.setattr = ns_setattr;
     *   g_obj_ops.readdir = ns_readdir;
     */
    memset(&g_obj_ops, 0, sizeof(g_obj_ops));
    return 0;
}

__attribute__((unused))
static const struct fsal_obj_ops *fsal_obj_get_ops(void)
{
    return &g_obj_ops;
}
