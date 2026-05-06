/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * log.c -- Component-based logging subsystem.
 */

#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>
#include <pthread.h>

#include "pnfs_mds.h"

enum log_level {
    LOG_FATAL = 0,
    LOG_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
    LOG_TRACE,
};

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

static const char *level_names[] = {
    "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE",
};

static const char *comp_names[] = {
    "MDS", "FSAL", "CLUSTER", "REPL", "CAT", "BPF", "NFS",
};

static enum log_level component_levels[LOG_COMP_COUNT];
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static FILE *log_file;

void mds_log_init(const char *path)
{
    if (path != NULL) {
        log_file = fopen(path, "a");
    }
    if (log_file == NULL) {
        log_file = stderr;
    }
    for (int i = 0; i < LOG_COMP_COUNT; i++) {
        component_levels[i] = LOG_INFO;
    }
}

void mds_log_set_level(int component, int level)
{
    if (component >= 0 && component < LOG_COMP_COUNT &&
        level >= LOG_FATAL && level <= LOG_TRACE) {
        component_levels[component] = (enum log_level)level;
    }
}

void mds_log(int component, int level, const char *fmt, ...)
{
    if (component < 0 || component >= LOG_COMP_COUNT) {
        return;
    }
    if (level > (int)component_levels[component]) {
        return;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    struct tm tm_buf;
    gmtime_r(&ts.tv_sec, &tm_buf);

    char time_buf[32];
    (void)strftime(time_buf, sizeof(time_buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);

    pthread_mutex_lock(&log_lock);

    (void)fprintf(log_file, "%s.%06ldZ [%s] %s: ",
            time_buf, ts.tv_nsec / 1000,
            comp_names[component],
            level_names[level]);

    va_list ap;
    va_start(ap, fmt);
    /* NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized) */
    (void)vfprintf(log_file, fmt, ap);
    va_end(ap);

    (void)fputc('\n', log_file);

    if (level <= LOG_ERROR) {
        (void)fflush(log_file);
    }

    pthread_mutex_unlock(&log_lock);
}

void mds_log_shutdown(void)
{
    if (log_file != NULL && log_file != stderr) {
        (void)fclose(log_file);
        log_file = NULL;
    }
}
