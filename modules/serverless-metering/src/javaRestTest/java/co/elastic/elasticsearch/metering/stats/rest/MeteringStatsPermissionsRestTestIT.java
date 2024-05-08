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

package co.elastic.elasticsearch.metering.stats.rest;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MeteringStatsPermissionsRestTestIT extends ESRestTestCase {
    private static final String PASSWORD = "secret-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("metering.index-info-task.enabled", "true")
        .user("test_admin", PASSWORD, "superuser", true)
        .user("test_foo_monitor", PASSWORD, "monitor_foo_indices", false)
        .user("test_bar_monitor", PASSWORD, "monitor_bar_indices", false)
        .user("test_all_monitor", PASSWORD, "monitor_all_indices", false)
        .user("test_no_privilege", PASSWORD, "no_privilege", false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        // If this test is running in a test framework that handles its own authorization, we don't want to overwrite it.
        if (super.restClientSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restClientSettings();
        } else {
            // Note: We use the admin user because the other one is too unprivileged, so it breaks the initialization of the test
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    private RequestOptions.Builder getFooRequestOptions() {
        return RequestOptions.DEFAULT.toBuilder()
            .addHeader("es-secondary-authorization", basicAuthHeaderValue("test_foo_monitor", new SecureString(PASSWORD.toCharArray())));
    }

    private RequestOptions.Builder getBarRequestOptions() {
        return RequestOptions.DEFAULT.toBuilder()
            .addHeader("es-secondary-authorization", basicAuthHeaderValue("test_bar_monitor", new SecureString(PASSWORD.toCharArray())));
    }

    private RequestOptions.Builder getNoPrivilegeRequestOptions() {
        return RequestOptions.DEFAULT.toBuilder()
            .addHeader("es-secondary-authorization", basicAuthHeaderValue("test_no_privilege", new SecureString(PASSWORD.toCharArray())));
    }

    @SuppressWarnings("unchecked")
    public void testGetMeteringStatsPrivileges() throws Exception {
        MeteringStatsRestTestIT.createAndLoadIndex("foo-test-1", Settings.EMPTY);
        MeteringStatsRestTestIT.createAndLoadIndex("bar-test-1", Settings.EMPTY);
        MeteringStatsRestTestIT.createAndLoadDatastreamWithRollover("foo-datastream-1", new HashMap<>(), new HashMap<>());
        MeteringStatsRestTestIT.createAndLoadDatastreamWithRollover("bar-datastream-1", new HashMap<>(), new HashMap<>());

        try (var client = buildClient(restAdminSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request meteringStatsRequest = new Request("GET", "/_metering/stats");
            meteringStatsRequest.setOptions(getFooRequestOptions());
            Response response = client.performRequest(meteringStatsRequest);
            Map<String, Object> responseMap = entityAsMap(response);
            List<Object> indices = (List<Object>) responseMap.get("indices");
            assertThat(
                "expected foo-test-1 and the two foo-datastream-1 indices, but got " + getPrintableIndexNames(indices),
                indices.size(),
                equalTo(3)
            );
            List<String> sortedIndexNames = indices.stream()
                .map(index -> ((Map<String, Object>) index).get("name").toString())
                .sorted()
                .toList();
            assertThat(sortedIndexNames.get(0), startsWith(".ds-foo-datastream-1"));
            assertThat(sortedIndexNames.get(1), startsWith(".ds-foo-datastream-1"));
            assertThat(sortedIndexNames.get(2), equalTo("foo-test-1"));

            List<Object> datastreams = (List<Object>) responseMap.get("datastreams");
            assertThat(datastreams.size(), equalTo(1));
            Map<String, Object> datastream = (Map<String, Object>) datastreams.get(0);
            String datastreamName = (String) datastream.get("name");
            assertThat(datastreamName, equalTo("foo-datastream-1"));

            final Request wildcardMeteringStatsRequest = new Request("GET", "/_metering/stats/bar*");
            wildcardMeteringStatsRequest.setOptions(getFooRequestOptions());
            ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> client.performRequest(wildcardMeteringStatsRequest)
            );
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(404));
            assertThat(responseException.getMessage(), containsString("no such index [bar*]"));

            Request badMeteringStatsRequest = new Request("GET", "/_metering/stats/bar-test-1");
            badMeteringStatsRequest.setOptions(getFooRequestOptions());
            responseException = expectThrows(ResponseException.class, () -> client.performRequest(badMeteringStatsRequest));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString(
                    "action [indices:monitor/get/metering/stats] is unauthorized for user [test_foo_monitor] with effective roles "
                        + "[monitor_foo_indices] on indices [bar-test-1]"
                )
            );

            {
                // Now test that with no secondary user, we get everything:
                meteringStatsRequest = new Request("GET", "/_metering/stats");
                response = client.performRequest(meteringStatsRequest);
                responseMap = entityAsMap(response);
                indices = (List<Object>) responseMap.get("indices");
                assertThat(
                    "expected foo-test-1 and the two foo-datastream-1 indices, but got " + getPrintableIndexNames(indices),
                    indices.size(),
                    equalTo(6)
                );
                sortedIndexNames = indices.stream().map(index -> ((Map<String, Object>) index).get("name").toString()).sorted().toList();
                assertThat(sortedIndexNames.get(0), startsWith(".ds-bar-datastream-1"));
                assertThat(sortedIndexNames.get(1), startsWith(".ds-bar-datastream-1"));
                assertThat(sortedIndexNames.get(2), startsWith(".ds-foo-datastream-1"));
                assertThat(sortedIndexNames.get(3), startsWith(".ds-foo-datastream-1"));
                assertThat(sortedIndexNames.get(4), equalTo("bar-test-1"));
                assertThat(sortedIndexNames.get(5), equalTo("foo-test-1"));

                datastreams = (List<Object>) responseMap.get("datastreams");
                assertThat(datastreams.size(), equalTo(2));
                List<String> sortedDatastreamNames = datastreams.stream()
                    .map(index -> ((Map<String, Object>) index).get("name").toString())
                    .sorted()
                    .toList();
                assertThat(sortedDatastreamNames.get(0), equalTo("bar-datastream-1"));
                assertThat(sortedDatastreamNames.get(1), equalTo("foo-datastream-1"));
            }
        }

        try (var client = buildClient(restAdminSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request meteringStatsRequest = new Request("GET", "/_metering/stats");
            meteringStatsRequest.setOptions(getBarRequestOptions());
            Response response = client.performRequest(meteringStatsRequest);
            Map<String, Object> responseMap = entityAsMap(response);
            List<Object> indices = (List<Object>) responseMap.get("indices");
            assertThat(
                "expected bar-test-1 and the two bar-datastream-1 indices, but got " + getPrintableIndexNames(indices),
                indices.size(),
                equalTo(3)
            );
            List<String> sortedIndexNames = indices.stream()
                .map(index -> ((Map<String, Object>) index).get("name").toString())
                .sorted()
                .toList();
            assertThat(sortedIndexNames.get(0), startsWith(".ds-bar-datastream-1"));
            assertThat(sortedIndexNames.get(1), startsWith(".ds-bar-datastream-1"));
            assertThat(sortedIndexNames.get(2), equalTo("bar-test-1"));

            List<Object> datastreams = (List<Object>) responseMap.get("datastreams");
            assertThat(datastreams.size(), equalTo(1));
            Map<String, Object> datastream = (Map<String, Object>) datastreams.get(0);
            String datastreamName = (String) datastream.get("name");
            assertThat(datastreamName, equalTo("bar-datastream-1"));
        }

        try (var client = buildClient(restAdminSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request meteringStatsRequest = new Request("GET", "/_metering/stats");
            meteringStatsRequest.setOptions(getNoPrivilegeRequestOptions());
            ResponseException responseException = expectThrows(ResponseException.class, () -> client.performRequest(meteringStatsRequest));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [indices:monitor/get/metering/stats] is unauthorized for user [test_no_privilege]")
            );
        }
    }

    /*
     * This takes in the "indices" list of the response map, and returns a sorted comma-delimited string of all of the names of the indices
     */
    @SuppressWarnings("unchecked")
    private String getPrintableIndexNames(List<Object> indices) {
        return indices.stream()
            .map(idx -> ((Map<String, Object>) idx).get("name").toString())
            .sorted()
            .collect(Collectors.joining(", ", "[", "]"));
    }
}
