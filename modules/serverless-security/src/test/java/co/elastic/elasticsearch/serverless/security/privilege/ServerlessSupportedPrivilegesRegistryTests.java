/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.serverless.security.privilege;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ServerlessSupportedPrivilegesRegistryTests extends ESTestCase {

    /**
     * The purpose of this test is to catch that any newly added cluster privilege to {@link ClusterPrivilegeResolver}
     * is properly classified as supported or not in Serverless.
     * <p>
     * If you have added a new privilege to {@link ClusterPrivilegeResolver}, please add it to
     * {@link ServerlessSupportedPrivilegesRegistry} if you'd also like to expose it to customers in Serverless.
     * If you don't want to expose it, add it to {@code unsupported} set below.
     * <p>
     * Note: If the new privilege is behind a feature flag, either implement a feature flag for it in the Serverless repo,
     * or add it to {@code unsupported} below.
     */
    public void testSupportedClusterPrivileges() {
        var unsupported = Set.of(
            "create_snapshot", // no customer-initiated snapshots
            "cross_cluster_replication", // no CCR
            "cross_cluster_search", // no CCS
            "delegate_pki", // no realm management
            "grant_api_key", // not necessary for customers (use manage_api_key or manage_own_api_key)
            "manage_autoscaling", // no customer-managed autoscaling
            "manage_ccr", // no CCR
            "manage_data_frame_transforms", // deprecated; use manage_transform
            "manage_data_stream_global_retention", // not supported yet
            "manage_ilm", // no ILM
            "manage_oidc", // no realm management
            "manage_rollup", // rollup deprecated in 8.11 and all rollup APIs blocked in serverless
            "manage_saml", // no realm management
            "manage_service_account", // no service account management
            "manage_slm", // no customer-initiated snapshots
            "manage_token", // tokens are only used internally
            "manage_user_profile", // user profile APIs are only used internally
            "manage_watcher", // no watcher
            "monitor_data_frame_transforms", // deprecated; use monitor_transform instead
            "monitor_data_stream_global_retention", // not supported yet
            "monitor_rollup", // "read" counterpart to manage_rollup
            "monitor_snapshot", // "read" counterpart to manage_slm
            "monitor_text_structure", // internal-only API
            "monitor_watcher", // no watcher
            "read_ccr", // "read" counterpart for manage_ccr
            "read_connector_secrets", // internal-only
            "read_fleet_secrets", // internal-only
            "read_ilm", // "read" counterpart to manage_ilm
            "read_slm", // "read" counterpart to manage_slm
            "transport_client", // no CCS/CCR
            "write_connector_secrets", // internal-only
            "write_fleet_secrets" // internal-only
        );
        var supported = ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames();
        var all = Sets.union(unsupported, supported);
        assertThat(all, equalTo(ClusterPrivilegeResolver.names()));
    }

    /**
     * The purpose of this test is to catch that any newly added index privilege to {@link IndexPrivilege}
     * is properly classified as supported or not in Serverless.
     * <p>
     * If you have added a new privilege to {@link IndexPrivilege}, please add it to
     * {@link ServerlessSupportedPrivilegesRegistry} if you'd also like to expose it to customers in Serverless.
     * If you don't want to expose it, add it to {@code unsupported} set below.
     * <p>
     * Note: If the new privilege is behind a feature flag, either implement a feature flag for it in the Serverless repo,
     * or add it to {@code unsupported} below.
     */
    public void testSupportedIndexPrivileges() {
        var unsupported = Set.of(
            "cross_cluster_replication", // no CCR
            "cross_cluster_replication_internal", // no CCR
            "manage_follow_index", // no CCR
            "manage_ilm", // no ILM
            "manage_leader_index", // no CCR
            "read_cross_cluster" // no CCS
        );
        var supported = ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames();
        var all = Sets.union(unsupported, supported);
        assertThat(all, equalTo(IndexPrivilege.names()));
    }
}
