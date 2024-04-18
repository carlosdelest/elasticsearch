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

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.NamedClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record ServerlessSupportedPrivilegesRegistry() {
    private static final Map<String, NamedClusterPrivilege> SUPPORTED_CLUSTER_PRIVILEGES = ClusterPrivilegeResolver.sortByAccessLevel(
        Set.of(
            ClusterPrivilegeResolver.ALL,
            ClusterPrivilegeResolver.CANCEL_TASK,
            ClusterPrivilegeResolver.MANAGE,
            ClusterPrivilegeResolver.MANAGE_API_KEY,
            ClusterPrivilegeResolver.MANAGE_BEHAVIORAL_ANALYTICS,
            ClusterPrivilegeResolver.MANAGE_ENRICH,
            ClusterPrivilegeResolver.MANAGE_IDX_TEMPLATES,
            ClusterPrivilegeResolver.MANAGE_INGEST_PIPELINES,
            ClusterPrivilegeResolver.MANAGE_LOGSTASH_PIPELINES,
            ClusterPrivilegeResolver.MANAGE_ML,
            ClusterPrivilegeResolver.MANAGE_OWN_API_KEY,
            ClusterPrivilegeResolver.MANAGE_PIPELINE,
            ClusterPrivilegeResolver.MANAGE_SEARCH_APPLICATION,
            ClusterPrivilegeResolver.MANAGE_SEARCH_QUERY_RULES,
            ClusterPrivilegeResolver.MANAGE_SEARCH_SYNONYMS,
            ClusterPrivilegeResolver.MANAGE_SECURITY,
            ClusterPrivilegeResolver.MANAGE_TRANSFORM,
            ClusterPrivilegeResolver.MONITOR,
            ClusterPrivilegeResolver.MONITOR_ENRICH,
            ClusterPrivilegeResolver.MONITOR_ML,
            ClusterPrivilegeResolver.MONITOR_TRANSFORM,
            ClusterPrivilegeResolver.NONE,
            ClusterPrivilegeResolver.POST_BEHAVIORAL_ANALYTICS_EVENT,
            ClusterPrivilegeResolver.READ_PIPELINE,
            ClusterPrivilegeResolver.READ_SECURITY
        )
    );

    private static final Map<String, IndexPrivilege> SUPPORTED_INDEX_PRIVILEGES = Privilege.sortByAccessLevel(
        Stream.of(
            IndexPrivilege.ALL,
            IndexPrivilege.AUTO_CONFIGURE,
            IndexPrivilege.CREATE,
            IndexPrivilege.CREATE_DOC,
            IndexPrivilege.CREATE_INDEX,
            IndexPrivilege.DELETE,
            IndexPrivilege.DELETE_INDEX,
            IndexPrivilege.INDEX,
            IndexPrivilege.MAINTENANCE,
            IndexPrivilege.MANAGE,
            IndexPrivilege.MANAGE_DATA_STREAM_LIFECYCLE,
            IndexPrivilege.MONITOR,
            IndexPrivilege.NONE,
            IndexPrivilege.READ,
            IndexPrivilege.VIEW_METADATA,
            IndexPrivilege.WRITE
        )
            .map(ServerlessSupportedPrivilegesRegistry::nameWithPrivilegeEntry)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    public static boolean isSupportedClusterPrivilege(String clusterPrivilegeName) {
        final String name = canonicalize(clusterPrivilegeName);
        final boolean supported = SUPPORTED_CLUSTER_PRIVILEGES.containsKey(name);
        assert false == supported || ClusterPrivilegeResolver.getNamedOrNull(name) != null;
        return supported;
    }

    public static boolean isSupportedIndexPrivilege(String indexPrivilegeName) {
        final String name = canonicalize(indexPrivilegeName);
        final boolean supported = SUPPORTED_INDEX_PRIVILEGES.containsKey(name);
        assert false == supported || IndexPrivilege.getNamedOrNull(name) != null;
        return supported;
    }

    public static Set<String> supportedClusterPrivilegeNames() {
        return SUPPORTED_CLUSTER_PRIVILEGES.keySet();
    }

    public static Set<String> supportedIndexPrivilegeNames() {
        return SUPPORTED_INDEX_PRIVILEGES.keySet();
    }

    private static String canonicalize(String name) {
        return Objects.requireNonNull(name).toLowerCase(Locale.ROOT);
    }

    private static Map.Entry<String, IndexPrivilege> nameWithPrivilegeEntry(IndexPrivilege privilege) {
        assert privilege.name().size() == 1 : "expected singleton but got [" + privilege.name() + "]";
        return Map.entry(privilege.name().iterator().next(), privilege);
    }

    public static Collection<String> findClusterPrivilegesThatGrant(
        Authentication authentication,
        String action,
        TransportRequest request
    ) {
        return findPrivilegesThatGrant(SUPPORTED_CLUSTER_PRIVILEGES, e -> e.getValue().permission().check(action, request, authentication));
    }

    public static Collection<String> findIndexPrivilegesThatGrant(String action) {
        return findPrivilegesThatGrant(SUPPORTED_INDEX_PRIVILEGES, e -> e.getValue().predicate().test(action));
    }

    private static <T> Collection<String> findPrivilegesThatGrant(
        Map<String, T> privileges,
        Predicate<Map.Entry<String, T>> privilegeEntryMatcher
    ) {
        return privileges.entrySet().stream().filter(privilegeEntryMatcher).map(Map.Entry::getKey).toList();
    }
}
