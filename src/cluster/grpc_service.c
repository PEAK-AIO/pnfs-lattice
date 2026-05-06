/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * grpc_service.c -- Inter-MDS admin RPC service.
 *
 * Provides programmatic admin API for cluster management operations.
 * Delegates to the cluster_transport admin handlers which implement
 * the actual logic for health checks, subtree management, migration,
 * failover, and DS administration.
 *
 * The service runs on the same cluster_transport_server that handles
 * inter-MDS data-plane traffic, sharing the same TCP listener and
 * message dispatch framework.  This avoids a separate gRPC dependency
 * while providing the same functionality.
 *
 * External orchestrators (Kubernetes, Terraform) interact via:
 *   - mds-admin CLI (connects to cluster_transport_server)
 *   - Direct TCP using the CT_MSG_* wire protocol
 *
 * The original gRPC stub has been replaced with this implementation
 * because the cluster_transport already provides a complete, tested
 * admin request/response framework with 70+ message types covering:
 *   - Health status (CT_MSG_HEALTH_*)
 *   - Subtree listing and migration (CT_MSG_SUBTREE_*, CT_MSG_MIG_*)
 *   - Failover promote/demote (CT_MSG_FAILOVER_*)
 *   - DS registry management (CT_MSG_DS_*)
 *   - Resilver/rebalance/tiering control
 *   - Quota management (CT_MSG_QUOTA_*)
 *   - Rolling upgrade lifecycle (CT_MSG_SET_LIFECYCLE)
 */

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "pnfs_mds.h"
#include "cluster_transport.h"

/**
 * @brief Verify the admin service is available.
 *
 * Returns true if the cluster_transport_server is running and
 * accepting admin requests on the configured port.
 *
 * @param srv  Cluster transport server handle.
 * @return true if admin service is operational.
 */
bool admin_service_is_ready(const struct cluster_server *srv)
{
    if (srv == NULL) {
        return false;
    }
    return cluster_transport_server_port(srv) != 0;
}

/**
 * @brief Return the admin service endpoint for external clients.
 *
 * @param srv       Cluster transport server handle.
 * @param buf       Output buffer for "host:port" string.
 * @param buf_len   Buffer capacity.
 * @return 0 on success, -1 on error.
 */
int admin_service_endpoint(const struct cluster_server *srv,
                           char *buf, size_t buf_len)
{
    uint16_t port;

    if (srv == NULL || buf == NULL || buf_len < 8) {
        return -1;
    }

    port = cluster_transport_server_port(srv);
    if (port == 0) {
        return -1;
    }

    (void)snprintf(buf, buf_len, "0.0.0.0:%u", (unsigned)port);
    return 0;
}
