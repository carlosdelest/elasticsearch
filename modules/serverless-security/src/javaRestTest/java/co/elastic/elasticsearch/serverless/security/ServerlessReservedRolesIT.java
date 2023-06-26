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

package co.elastic.elasticsearch.serverless.security;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.AnnotationTestOrdering;
import org.elasticsearch.test.AnnotationTestOrdering.Order;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

@TestCaseOrdering(AnnotationTestOrdering.class)
public class ServerlessReservedRolesIT extends ESRestTestCase {

    private static MutableSettingsProvider clusterSettings = new MutableSettingsProvider() {
        {
            put("stateless.enabled", "true");
            put("xpack.security.operator_privileges.enabled", "true");
            put("stateless.object_store.type", "fs");
            put("stateless.object_store.bucket", "stateless");
            put("stateless.object_store.base_path", "base_path");
            put("xpack.security.enabled", "true");
            put("ingest.geoip.downloader.enabled", "false");
            put("xpack.searchable.snapshot.shared_cache.size", "16MB");
            put("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
        }
    };

    private static final String OPERATOR_USER = "x_pack_rest_user";
    private static final String OPERATOR_PASSWORD = "x-pack-test-password";

    private static Set<String> INCLUDED_RESERVED_ROLES;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("javaRestTest")
        .settings(clusterSettings)
        .user(OPERATOR_USER, OPERATOR_PASSWORD)
        .configFile("operator_users.yml", Resource.fromClasspath("operator_users.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, new SecureString(OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Order(10)
    public void testDefaultReservedRoles() throws Exception {
        final Response response = adminClient().performRequest(new Request("GET", "/_security/role"));
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(
            responseMap.keySet(),
            equalTo(
                Set.of(
                    "apm_system",
                    "apm_user",
                    "beats_admin",
                    "beats_system",
                    "data_frame_transforms_admin",
                    "data_frame_transforms_user",
                    "editor",
                    "enrich_user",
                    "ingest_admin",
                    "kibana_admin",
                    "kibana_system",
                    "kibana_user",
                    "logstash_admin",
                    "logstash_system",
                    "machine_learning_admin",
                    "machine_learning_user",
                    "monitoring_user",
                    "remote_monitoring_agent",
                    "remote_monitoring_collector",
                    "reporting_user",
                    "rollup_admin",
                    "rollup_user",
                    "snapshot_user",
                    "superuser",
                    "transform_admin",
                    "transform_user",
                    "transport_client",
                    "viewer",
                    "watcher_admin",
                    "watcher_user"
                )
            )
        );
    }

    @Order(20)
    public void testRestartForConfigurableReservedRoles() throws IOException {
        INCLUDED_RESERVED_ROLES = new HashSet<>();
        INCLUDED_RESERVED_ROLES.add("superuser");
        INCLUDED_RESERVED_ROLES.addAll(
            randomSubsetOf(List.of("editor", "viewer", "kibana_system", "apm_system", "beats_system", "logstash_system"))
        );
        clusterSettings.put("xpack.security.reserved_roles.include", Strings.collectionToCommaDelimitedString(INCLUDED_RESERVED_ROLES));
        cluster.restart(false);
        closeClients();
    }

    @Order(30)
    public void testConfigurableReservedRoles() throws Exception {
        assert INCLUDED_RESERVED_ROLES != null;
        final Response response = adminClient().performRequest(new Request("GET", "/_security/role"));
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.keySet(), equalTo(INCLUDED_RESERVED_ROLES));
    }
}
