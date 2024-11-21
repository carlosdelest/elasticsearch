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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.cluster.serverless.local.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class MeteringStatsIndexSizeRestTestIT extends MeteringStatsRestTestCase {

    @Before
    public void resetClient() throws IOException {
        closeClients();
        initClient();
    }

    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .withNode(node("index2", "index"))// first node created by default
        .withNode(node("index3", "index"))
        .withNode(node("search2", "search"))// first node created by default
        .name("javaRestTest")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .setting("serverless.project_type", "ELASTICSEARCH_SEARCH")
        // speed things up a bit
        .setting("metering.index-info-task.poll.interval", "1s")
        .setting("serverless.autoscaling.search_metrics.push_interval", "500ms")
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testGetMeteringStats() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        RequestOptions.Builder requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "es-secondary-authorization",
                basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()))
            );
        {
            /*
             * First we load some indices and data streams into the cluster, and make sure we get the expected stats back
             */
            final int numIndices = randomIntBetween(1, 10);
            AtomicInteger totalDocs = new AtomicInteger(0);
            Map<String, Integer> indexNameToNumDocsMap = new HashMap<>();
            for (int i = 0; i < numIndices; i++) {
                String indexName = "test_index_" + i;
                int numDocs = createAndLoadIndex(indexName, settings);
                indexNameToNumDocsMap.put(indexName, numDocs);
                totalDocs.addAndGet(numDocs);
            }
            final int numDatastreams = randomIntBetween(0, 10);
            Map<String, Integer> datastreamNameToNumDocsMap = new HashMap<>();
            Map<String, String> datastreamIndexToDatastreamMap = new HashMap<>();
            for (int i = 0; i < numDatastreams; i++) {
                String datastreamName = "test_datastream_" + i;
                int numDocs = createAndLoadDatastreamWithRollover(datastreamName, indexNameToNumDocsMap, datastreamIndexToDatastreamMap);
                datastreamNameToNumDocsMap.put(datastreamName, numDocs);
                totalDocs.addAndGet(numDocs);
            }

            assertBusy(() -> {
                Request getMeteringStatsRequest = new Request("GET", "/_metering/stats");
                getMeteringStatsRequest.setOptions(requestOptionsBuilder);
                Response response = client().performRequest(getMeteringStatsRequest);
                Map<String, Object> responseMap = entityAsMap(response);

                Map<String, Object> total = (Map<String, Object>) responseMap.get("_total");
                assertThat(total.get("num_docs"), equalTo(totalDocs.get()));
                int size = (int) total.get("size_in_bytes");
                assertThat(size, is(greaterThanOrEqualTo(totalDocs.get())));

                List<Object> indices = (List<Object>) responseMap.get("indices");
                assertThat(indices.size(), equalTo(numIndices + (2 * numDatastreams)));
                for (int i = 0; i < numIndices; i++) {
                    Map<String, Object> index = (Map<String, Object>) indices.get(i);
                    String indexName = (String) index.get("name");
                    var expectedIndexDocs = indexNameToNumDocsMap.get(indexName);
                    assertThat(index.get("num_docs"), equalTo(expectedIndexDocs));
                    int indexSize = (int) index.get("size_in_bytes");
                    assertThat(indexSize, is(greaterThanOrEqualTo(expectedIndexDocs)));
                    if (datastreamIndexToDatastreamMap.containsKey(indexName)) {
                        assertThat(index.get("datastream"), equalTo(datastreamIndexToDatastreamMap.get(indexName)));
                    } else {
                        assertThat(index.containsKey("datastream"), equalTo(false));
                    }
                }

                List<Object> datastreams = (List<Object>) responseMap.get("datastreams");
                assertThat(datastreams.size(), equalTo(numDatastreams));
                for (int i = 0; i < numDatastreams; i++) {
                    Map<String, Object> index = (Map<String, Object>) datastreams.get(i);
                    String datastreamName = (String) index.get("name");
                    int expectedDocsCount = datastreamNameToNumDocsMap.get(datastreamName);
                    assertThat(index.get("num_docs"), equalTo(expectedDocsCount));
                    int indexSize = (int) index.get("size_in_bytes");
                    assertThat(indexSize, is(greaterThanOrEqualTo(expectedDocsCount)));
                    assertThat(index.containsKey("datastream"), equalTo(false));
                }
            });
        }

        {
            /*
             * Now we load a few indices and make sure the pattern-matching works
             */
            int foo1Docs = createAndLoadIndex("foo1", settings);
            int foo2Docs = createAndLoadIndex("foo2", settings);
            int fooDsDocs = createAndLoadDatastreamWithRollover("foo-ds-1", new HashMap<>(), new HashMap<>());
            int barDocs = createAndLoadIndex("bar", settings);

            assertBusy(() -> {
                Request getMeteringStatsRequest = new Request("GET", "/_metering/stats/foo*,bar*");
                getMeteringStatsRequest.setOptions(requestOptionsBuilder);
                Response response = client().performRequest(getMeteringStatsRequest);
                Map<String, Object> responseMap = entityAsMap(response);
                Map<String, Object> total = (Map<String, Object>) responseMap.get("_total");
                assertThat(total.get("num_docs"), equalTo(foo1Docs + foo2Docs + fooDsDocs + barDocs));
                assertThat(((List<Object>) responseMap.get("indices")).size(), equalTo(5));
                assertThat(((List<Object>) responseMap.get("datastreams")).size(), equalTo(1));

                getMeteringStatsRequest = new Request("GET", "/_metering/stats/foo*");
                getMeteringStatsRequest.setOptions(requestOptionsBuilder);
                response = client().performRequest(getMeteringStatsRequest);
                responseMap = entityAsMap(response);
                total = (Map<String, Object>) responseMap.get("_total");
                assertThat(total.get("num_docs"), equalTo(foo1Docs + foo2Docs + fooDsDocs));
                assertThat(((List<Object>) responseMap.get("indices")).size(), equalTo(4));
                assertThat(((List<Object>) responseMap.get("datastreams")).size(), equalTo(1));

                getMeteringStatsRequest = new Request("GET", "/_metering/stats/foo1");
                getMeteringStatsRequest.setOptions(requestOptionsBuilder);
                response = client().performRequest(getMeteringStatsRequest);
                responseMap = entityAsMap(response);
                total = (Map<String, Object>) responseMap.get("_total");
                assertThat(total.get("num_docs"), equalTo(foo1Docs));
                assertThat(((List<Object>) responseMap.get("indices")).size(), equalTo(1));
                assertThat(((List<Object>) responseMap.get("datastreams")).size(), equalTo(0));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetMeteringStatsNoIndexMatch() throws Exception {

        RequestOptions.Builder requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "es-secondary-authorization",
                basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()))
            );

        /*
         * Make sure our cluster is clean
         */
        wipeAllIndices();
        wipeDataStreams();

        assertBusy(() -> {
            Request getMeteringStatsRequest = new Request("GET", "/_metering/stats");
            getMeteringStatsRequest.setOptions(requestOptionsBuilder);
            var ex = expectThrows(ResponseException.class, () -> client().performRequest(getMeteringStatsRequest));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        });
    }
}
