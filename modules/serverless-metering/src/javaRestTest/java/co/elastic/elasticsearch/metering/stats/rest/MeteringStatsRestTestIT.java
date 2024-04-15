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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.cluster.serverless.local.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.equalTo;

public class MeteringStatsRestTestIT extends ESRestTestCase {

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
        .setting("metering.index-info-task.enabled", "true")
        .setting("metering.index-info-task.poll.interval", "5s")
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
            final int numIndices = randomIntBetween(0, 10);
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
                assertThat(total.containsKey("size_in_bytes"), equalTo(true));

                List<Object> indices = (List<Object>) responseMap.get("indices");
                assertThat(indices.size(), equalTo(numIndices + (2 * numDatastreams)));
                for (int i = 0; i < numIndices; i++) {
                    Map<String, Object> index = (Map<String, Object>) indices.get(i);
                    String indexName = (String) index.get("name");
                    assertThat(index.get("num_docs"), equalTo(indexNameToNumDocsMap.get(indexName)));
                    assertThat(index.containsKey("size_in_bytes"), equalTo(true));
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
                    assertThat(index.get("num_docs"), equalTo(datastreamNameToNumDocsMap.get(datastreamName)));
                    assertThat(index.containsKey("size_in_bytes"), equalTo(true));
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
                assertThat(total.containsKey("size_in_bytes"), equalTo(true));
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

    static int createAndLoadIndex(String indexName, Settings settings) throws IOException {
        if (randomBoolean()) {
            settings = Settings.builder().put(settings).put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();
        }
        createIndex(adminClient(), indexName, settings);
        int numDocs = randomIntBetween(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        adminClient().performRequest(bulkRequest);

        ensureGreen(adminClient(), indexName);

        adminClient().performRequest(new Request("POST", "/" + indexName + "/_flush"));
        return numDocs;
    }

    private static void ensureGreen(RestClient client, String index) throws IOException {
        ensureHealth(client, index, (request) -> {
            request.addParameter("wait_for_status", "green");
            request.addParameter("wait_for_no_relocating_shards", "true");
            request.addParameter("level", "shards");
        });
    }

    @SuppressWarnings("unchecked")
    public static int createAndLoadDatastreamWithRollover(
        String datastreamName,
        Map<String, Integer> indexNameToNumDocsMap,
        Map<String, String> datastreamIndexToDatastreamMap
    ) throws IOException {
        Request templateRequest = new Request("PUT", "/_index_template/" + datastreamName + "-template");
        String templateBody = Strings.format("""
            {
              "index_patterns": ["%s*"],
              "data_stream": { },
              "template": {
                "lifecycle": {
                  "data_retention": "7d"
                }
              }
            }
            """, datastreamName);
        templateRequest.setJsonEntity(templateBody);
        adminClient().performRequest(templateRequest);

        int numDocs1 = randomIntBetween(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs1; i++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\": \"2099-05-06T16:21:15.000Z\", \"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + datastreamName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        Map<String, Object> bulkResponse = entityAsMap(adminClient().performRequest(bulkRequest));
        List<Map<String, Map<String, Object>>> items = (List<Map<String, Map<String, Object>>>) bulkResponse.get("items");
        assertThat(items.size(), equalTo(numDocs1));
        for (Map<String, Map<String, Object>> item : items) {
            String indexName = (String) item.get("create").get("_index");
            indexNameToNumDocsMap.compute(indexName, (index, currentDocCount) -> currentDocCount == null ? 1 : currentDocCount + 1);
            datastreamIndexToDatastreamMap.put(indexName, datastreamName);
        }

        Request rolloverRequest = new Request("POST", "/" + datastreamName + "/_rollover");
        adminClient().performRequest(rolloverRequest);
        ensureGreen(adminClient(), datastreamName);

        int numDocs2 = randomIntBetween(1, 100);
        bulk = new StringBuilder();
        for (int i = 0; i < numDocs2; i++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\": \"2099-05-06T16:21:15.000Z\", \"foo\": \"bar\"}\n");
        }
        bulkRequest = new Request("POST", "/" + datastreamName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkResponse = entityAsMap(adminClient().performRequest(bulkRequest));
        items = (List<Map<String, Map<String, Object>>>) bulkResponse.get("items");
        for (Map<String, Map<String, Object>> item : items) {
            String indexName = (String) item.get("create").get("_index");
            indexNameToNumDocsMap.compute(indexName, (index, currentDocCount) -> currentDocCount == null ? 1 : currentDocCount + 1);
            datastreamIndexToDatastreamMap.put(indexName, datastreamName);
        }

        adminClient().performRequest(new Request("POST", "/" + datastreamName + "/_flush"));
        return numDocs1 + numDocs2;
    }
}
