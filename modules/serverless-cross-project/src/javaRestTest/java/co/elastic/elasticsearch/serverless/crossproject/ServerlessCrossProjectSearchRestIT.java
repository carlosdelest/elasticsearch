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

package co.elastic.elasticsearch.serverless.crossproject;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This test currently uses RCS1 to exercise searching across clusters. This will need to be rewritten to use CPS instead as the
 * functionality becomes available.
 */
public class ServerlessCrossProjectSearchRestIT extends ESRestTestCase {

    private static final String LINKED_CLUSTER = "my_linked_cluster";
    private static final String USER = "user";
    private static final String LINKED_SEARCH_USER = "linked_search_user";
    private static final String LINKED_SEARCH_ROLE = "linked_search";
    private static final String LINKED_METRIC_USER = "linked_metric_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());

    private static final ServerlessElasticsearchCluster linkedCluster = ServerlessElasticsearchCluster.local()
        .name(LINKED_CLUSTER)
        .user(USER, PASS.toString())
        .user(LINKED_METRIC_USER, PASS.toString(), "read_linked_shared_metrics", false)
        .user(LINKED_SEARCH_USER, PASS.toString(), LINKED_SEARCH_ROLE, false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    private static final ServerlessElasticsearchCluster originCluster = ServerlessElasticsearchCluster.local()
        .name("origin-cluster")
        .user(USER, PASS.toString())
        .user(LINKED_METRIC_USER, PASS.toString(), "read_linked_shared_metrics", false)
        .user(LINKED_SEARCH_USER, PASS.toString(), LINKED_SEARCH_ROLE, false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .configFile("operator/settings.json", Resource.fromString(() -> {
            var address = linkedCluster.getTransportEndpoint(0);
            return Strings.format("""
                {
                  "metadata": {
                    "version": "1",
                    "compatibility": "8.4.0"
                  },
                  "state": {
                    "cluster_settings": {
                      "cluster.remote.my_linked_cluster.mode": "proxy",
                      "cluster.remote.my_linked_cluster.proxy_address": "%s",
                      "cluster.remote.my_linked_cluster.skip_unavailable": "false"
                    }
                  }
                }
                """, address);
        }))
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(linkedCluster).around(originCluster);

    private static RestClient linkedClusterClient;

    @BeforeClass
    public static void initLinkedClusterClient() {
        if (linkedClusterClient == null) {
            linkedClusterClient = buildRestClient(linkedCluster);
        }
    }

    private static RestClient buildRestClient(ElasticsearchCluster targetCluster) {
        assert targetCluster != null;
        final int numberOfFcNodes = targetCluster.getHttpAddresses().split(",").length;
        final String url = targetCluster.getHttpAddress(randomIntBetween(0, numberOfFcNodes - 1));

        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(url.substring(0, portSeparator), Integer.parseInt(url.substring(portSeparator + 1)), "http");
        RestClientBuilder builder = RestClient.builder(httpHost);
        try {
            doConfigureClient(builder, Settings.EMPTY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    @AfterClass
    public static void closeLinkedClusterClient() throws Exception {
        try {
            IOUtils.close(linkedClusterClient);
        } finally {
            linkedClusterClient = null;
        }
    }

    @Override
    protected String getTestRestCluster() {
        return originCluster.getHttpAddress(0);
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", basicAuthHeaderValue(USER, PASS)).build();
    }

    public void testCrossProjectSearch() throws Exception {
        checkRemoteConnection(LINKED_CLUSTER);
        {
            // Index some documents, so we can attempt to search them from the origin cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "index2" } }
                { "bar": "foo" }
                { "index": { "_index": "prefixed_index" } }
                { "baz": "fee" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }
                """));
            assertOK(performRequest(linkedClusterClient, USER, PASS, bulkRequest));
        }

        {
            // Index some documents, to use them in a mixed-cluster search
            final var indexDocRequest = new Request("POST", "/origin_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"origin_foo\": \"origin_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Check that we can search the linked cluster from the origin cluster
            final boolean alsoSearchOrigin = randomBoolean();
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search",
                    alsoSearchOrigin ? "origin_index," : "",
                    randomFrom(LINKED_CLUSTER, "*", "my_linked_*"),
                    randomFrom("index1", "*")
                )
            );
            if (alsoSearchOrigin) {
                searchAndAssertIndicesInResult(searchRequest, LINKED_SEARCH_USER, PASS, "index1", "origin_index");
            } else {
                searchAndAssertIndicesInResult(searchRequest, LINKED_SEARCH_USER, PASS, "index1");
            }

            final var metricSearchRequest = new Request("GET", Strings.format("/%s:*/_search", LINKED_CLUSTER));
            searchAndAssertIndicesInResult(metricSearchRequest, LINKED_METRIC_USER, PASS, "shared-metrics");

            // Check that access is denied because of user privileges
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequest(
                    client(),
                    LINKED_SEARCH_USER,
                    PASS,
                    new Request("GET", Strings.format("/%s:index2/_search", LINKED_CLUSTER))
                )
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception.getMessage(),
                containsString(
                    "action [indices:data/read/search] is unauthorized for user [linked_search_user] with "
                        + "effective roles [linked_search] on indices [index2], this action is granted by the index privileges [read,all]"
                )
            );
        }
    }

    private static void searchAndAssertIndicesInResult(Request searchRequest, String user, SecureString password, String... indices)
        throws Exception {
        final var response = performRequest(client(), user, password, searchRequest);
        assertOK(response);
        final var searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            final var actualIndices = Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).distinct().toList();
            assertThat(actualIndices, containsInAnyOrder(indices));
        } finally {
            searchResponse.decRef();
        }
    }

    private static Response performRequest(RestClient targetClusterClient, String user, SecureString password, Request request)
        throws IOException {
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, password)));
        return targetClusterClient.performRequest(request);
    }

    private void checkRemoteConnection(String clusterAlias) throws Exception {
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final var remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            final var remoteInfoObjectPath = assertOKAndCreateObjectPath(remoteInfoResponse);
            assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".connected"), is(true));
        });
    }
}
