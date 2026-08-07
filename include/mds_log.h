/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_log.h -- Component-based leveled logging interface.
 *
 * The implementation lives in src/common/log.c.  This header is kept
 * dependency-free so any translation unit can include it directly and
 * emit diagnostics through the MDS_LOG_* macros.
 */

#ifndef MDS_LOG_H
#define MDS_LOG_H

/*
 * Severity levels, ordered most-severe (0) to least-severe.  A record
 * is emitted only when the component's configured level is numerically
 * >= the record's level (see mds_log()), so a component set to LOG_WARN
 * passes FATAL/ERROR/WARN and drops INFO/DEBUG/TRACE.
 */
enum log_level {
    LOG_FATAL = 0,
    LOG_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
    LOG_TRACE,
};

/* Logical subsystems.  Each carries an independent verbosity level. */
enum log_component {
    LOG_COMP_MDS = 0,
    LOG_COMP_FSAL,
    LOG_COMP_CLUSTER,
    LOG_COMP_REPL,
    LOG_COMP_CAT,
    LOG_COMP_BPF,
    LOG_COMP_NFS,
    LOG_COMP_COUNT,
};

#if defined(__GNUC__) || defined(__clang__)
# define MDS_LOG_PRINTF(fmt_idx, arg_idx) \
    __attribute__((format(printf, fmt_idx, arg_idx)))
#else
# define MDS_LOG_PRINTF(fmt_idx, arg_idx)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialise logging.
 * @param path  Output file path; NULL or "" sends output to stderr.
 *              A non-NULL path is opened in append mode; on failure the
 *              logger falls back to stderr.  All components default to
 *              LOG_INFO.
 */
void mds_log_init(const char *path);

/**
 * @brief Set the verbosity level for a single component.
 * @param component  An enum log_component value.
 * @param level      An enum log_level value.
 * Out-of-range arguments are ignored.
 */
void mds_log_set_level(int component, int level);

/**
 * @brief Emit a timestamped record if level is enabled for component.
 * @param component  An enum log_component value.
 * @param level      An enum log_level value.
 * @param fmt        printf-style format string.
 */
void mds_log(int component, int level, const char *fmt, ...)
    MDS_LOG_PRINTF(3, 4);

/** @brief Flush and close the log file (no-op when logging to stderr). */
void mds_log_shutdown(void);

/**
 * @brief Parse a level token, case-insensitive.
 * @param s  One of "fatal","error","warn","info","debug","trace".
 * @return The enum log_level value, or -1 if unknown.
 */
int mds_log_level_from_str(const char *s);

/**
 * @brief Parse a component token, case-insensitive.
 * @param s  One of "mds","fsal","cluster","repl","cat","bpf","nfs".
 * @return The enum log_component value, or -1 if unknown.
 */
int mds_log_component_from_str(const char *s);

/*
 * Per-component verbosity levels backing the MDS_LOG_* fast-path gate
 * below.  Owned by log.c: written only by mds_log_init() and
 * mds_log_set_level() (relaxed atomic stores).  Treat as read-only
 * everywhere else -- always change levels via mds_log_set_level().
 */
extern int mds_log_component_levels[LOG_COMP_COUNT];

#ifdef __cplusplus
}
#endif

/*
 * Ergonomic wrappers.  The component is explicit at every call site so
 * diagnostics read e.g. MDS_LOG_WARN(LOG_COMP_NFS, "bad seqid %u", n).
 * Macro names are intentionally distinct from the LOG_<LEVEL> enum
 * constants so the two never collide.
 *
 * The level gate lives HERE, not inside mds_log(): a suppressed
 * DEBUG/TRACE call site costs one relaxed load and a predicted-not-
 * taken branch instead of a varargs call into mds_log().  mds_log()
 * keeps its own internal check for direct callers.  The relaxed
 * atomic load pairs with the atomic stores in mds_log_set_level();
 * __atomic_load_n is used (not <stdatomic.h>) because this header
 * compiles in both C and C++ translation units.
 */
#if defined(__GNUC__) || defined(__clang__)
# define MDS_LOG_LEVEL_LOAD_(comp) \
    __atomic_load_n(&mds_log_component_levels[(comp)], __ATOMIC_RELAXED)
#else
# define MDS_LOG_LEVEL_LOAD_(comp) (mds_log_component_levels[(comp)])
#endif

#define MDS_LOG_AT_(comp, lvl, ...) \
    do { \
        int mds_log_comp_ = (comp); \
        if ((unsigned)mds_log_comp_ < (unsigned)LOG_COMP_COUNT && \
            (int)(lvl) <= MDS_LOG_LEVEL_LOAD_(mds_log_comp_)) { \
            mds_log(mds_log_comp_, (lvl), __VA_ARGS__); \
        } \
    } while (0)

#define MDS_LOG_FATAL(comp, ...) MDS_LOG_AT_((comp), LOG_FATAL, __VA_ARGS__)
#define MDS_LOG_ERROR(comp, ...) MDS_LOG_AT_((comp), LOG_ERROR, __VA_ARGS__)
#define MDS_LOG_WARN(comp, ...)  MDS_LOG_AT_((comp), LOG_WARN,  __VA_ARGS__)
#define MDS_LOG_INFO(comp, ...)  MDS_LOG_AT_((comp), LOG_INFO,  __VA_ARGS__)
#define MDS_LOG_DEBUG(comp, ...) MDS_LOG_AT_((comp), LOG_DEBUG, __VA_ARGS__)
#define MDS_LOG_TRACE(comp, ...) MDS_LOG_AT_((comp), LOG_TRACE, __VA_ARGS__)

#endif /* MDS_LOG_H */
