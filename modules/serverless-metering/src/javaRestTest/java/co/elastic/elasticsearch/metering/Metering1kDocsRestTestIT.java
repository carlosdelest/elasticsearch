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

package co.elastic.elasticsearch.metering;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.test.cluster.serverless.local.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class Metering1kDocsRestTestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(MeteringRestTestIT.class);

    static int REPORT_PERIOD = 5;
    String indexName = "test_index_1";

    @ClassRule
    public static UsageApiTestServer usageApiTestServer = new UsageApiTestServer();

    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .withNode(node("index2", "index"))// first node created by default
        .withNode(node("index3", "index"))
        .withNode(node("search2", "search"))// first node created by default
        .name("javaRestTest")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .setting("metering.project_id", "testProjectId")
        .setting("metering.url", "http://localhost:" + usageApiTestServer.getAddress().getPort())
        .setting("metering.report_period", REPORT_PERIOD + "s") // speed things up a bit
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

    public void testMeteringRecordsCanBeDeduplicated() throws Exception {
        // This test asserts the ingested doc metric for 1k documents sums up to consistent value
        // this test also asserts about an exact value of index-size metrics. To make sure, that assertion
        // is consistent a shard has to be merged to a 1 segment. Then a value from /_cat/segments
        // is used as an expected value
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        createIndex(indexName, settings);

        // ingest more docs so that each shard has some
        int numDocs = 1000;

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        ensureGreen(indexName);

        forceMerge();

        logShardAllocationInformation(indexName);

        List<Map<?, ?>> ingestedDocs = new ArrayList<>();
        assertBusy(() -> {
            // ingested-doc metrics are emitted only once.
            // we need to await all are sent.
            var records = usageApiTestServer.getUsageRecords("ingested-doc");
            ingestedDocs.addAll(records);
            int sum = sumQuantity(ingestedDocs);

            // asserting that eventually records value sum up to expected value
            assertThat(sum, equalTo(numDocs * 6 /* size in bytes of the single doc*/));
            logger.info(numDocs);
            logger.info(ingestedDocs.size());
            logger.info(sum);
        });
        assertBusy(() -> {
            var allUsageRecords = usageApiTestServer.getAllUsageRecords();
            var ingestedDocsRecords = UsageApiTestServer.filterUsageRecords(allUsageRecords, "ingested-doc");
            // once we asserted total size numDocs*96 there will be no more records
            assertThat(ingestedDocsRecords, empty());

            var shardSizeRecords = UsageApiTestServer.filterUsageRecords(allUsageRecords, "shard-size");

            // there might be records from multiple metering.report_period. We are interested in the latest
            // because the replication might take time and also some nodes might be reporting with a delay

            // we are expecting 6 records in a full batch. 3 primaries per each replica.
            var latestTimestampWithSixRecords = getLatestFullBatch(shardSizeRecords, 6);
            List<Map<?, ?>> latestRecords = latestTimestampWithSixRecords.getValue();
            logger.info(
                debugInfoForShardSize(
                    indexName,
                    latestTimestampWithSixRecords.getKey(),
                    latestTimestampWithSixRecords.getValue(),
                    shardSizeRecords
                )
            );

            // those are the expected values taken from the /_cat/segments api
            // we are asserting that there will be only 1 segment per shard and taking its sizeInBytes
            Map<String, Map<Integer, Integer>> nodeToShardToSize = getExpectedReplicaSizes();

            Map<String, List<Map<?, ?>>> groupByNode = groupByNodeName(latestRecords);
            // there are 2 replicas, so records should be from 2 nodes only
            assertThat(groupByNode.size(), equalTo(2));
            assertThat(groupByNode.keySet(), equalTo(nodeToShardToSize.keySet()));

            Iterator<String> iterator = groupByNode.keySet().iterator();
            var searchNodeId1 = iterator.next();
            var searchNodeId2 = iterator.next();

            // there are 3 primary shards, so should be 3 unique ids per each node
            assertThat(groupByNode.get(searchNodeId1).size(), equalTo(3));
            assertThat(groupByNode.get(searchNodeId2).size(), equalTo(3));

            // all the quantities reported on a replicas should be the same as from _cat/segments api
            assertThat(
                groupByNode + " vs + " + nodeToShardToSize,
                shardNumberToSize(groupByNode.get(searchNodeId1)),
                equalTo(nodeToShardToSize.get(searchNodeId1))
            );
            assertThat(
                groupByNode + " vs + " + nodeToShardToSize,
                shardNumberToSize(groupByNode.get(searchNodeId2)),
                equalTo(nodeToShardToSize.get(searchNodeId2))
            );

        }, 30, TimeUnit.SECONDS);

    }

    private void forceMerge() throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        request.addParameter("flush", "true");
        client().performRequest(request);
    }

    private Map<Integer, Integer> shardNumberToSize(List<Map<?, ?>> usageRecords) {
        Map<Integer, Integer> shardNumberToSize = new HashMap<>();
        usageRecords.forEach(record -> {
            var shardNumber = Integer.parseInt((String) XContentMapValues.extractValue("source.metadata.shard", record));
            var shardSize = (int) XContentMapValues.extractValue("usage.quantity", record);
            shardNumberToSize.put(shardNumber, shardSize);
        });
        return shardNumberToSize;
    }

    private Map<String, List<Map<?, ?>>> groupByNodeName(List<Map<?, ?>> latestRecords) {
        Map<String, List<Map<?, ?>>> usagesByNodeName = new HashMap<>();
        latestRecords.forEach(record -> {
            String nodeName = (String) XContentMapValues.extractValue("source.id", record);
            List<Map<?, ?>> recordsForNodeName = usagesByNodeName.computeIfAbsent(nodeName, k -> new ArrayList<>());
            recordsForNodeName.add(record);
        });
        return usagesByNodeName;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<Integer, Integer>> getExpectedReplicaSizes() throws IOException {

        Map<String, Object> indices = entityAsMap(client().performRequest(new Request("GET", indexName + "/_segments")));

        Map<String, List<Map<String, ?>>> shards = (Map<String, List<Map<String, ?>>>) XContentMapValues.extractValue(
            "indices." + indexName + ".shards",
            indices
        );

        Map<String, Map<Integer, Integer>> nodeToShardNumberToSize = new HashMap<>();
        shards.forEach((shardNumber, shardCopies) -> {
            shardCopies.forEach(shardCopyInfo -> {
                // skipping primaries info
                if (((boolean) XContentMapValues.extractValue("routing.primary", shardCopyInfo)) == false) {
                    var nodeName = "es-" + XContentMapValues.extractValue("routing.node", shardCopyInfo);

                    var segments = (Map<String, ?>) XContentMapValues.extractValue("segments", shardCopyInfo);
                    assert segments.size() == 1;// important, we expect segments to be merged to 1
                    Map<Integer, Integer> shardNumberToSize = nodeToShardNumberToSize.computeIfAbsent(nodeName, k -> new HashMap<>());
                    var segment = (Map<String, ?>) segments.values().iterator().next();
                    shardNumberToSize.put(Integer.parseInt(shardNumber), (Integer) segment.get("size_in_bytes"));
                }

            });
        });

        return nodeToShardNumberToSize;
    }

    private String debugInfoForShardSize(
        String indexName,
        Instant timestamp,
        List<Map<?, ?>> latestRecords,
        List<Map<?, ?>> shardSizeRecords
    ) throws IOException {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Latest timestamp with 6 records " + timestamp);
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append("Latest records " + latestRecords);
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append("All usage records for shard-size " + shardSizeRecords);
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append(prepareShardAllocationInformation(indexName));
        return msgBuilder.toString();
    }

    private static int sumQuantity(List<Map<?, ?>> ingestedDocs) {
        return ingestedDocs.stream().mapToInt(m -> (Integer) ((Map<?, ?>) m.get("usage")).get("quantity")).sum();
    }

    private static Map.Entry<Instant, List<Map<?, ?>>> getLatestFullBatch(List<Map<?, ?>> metric, int expectedNumberOfRecords) {
        Map<?, List<Map<?, ?>>> groupedByTimestamp = metric.stream().collect(groupingBy(m -> m.get("usage_timestamp")));
        Map<Instant, List<Map<?, ?>>> fullBatchesOnly = groupedByTimestamp.entrySet()
            .stream()
            .filter(e -> e.getValue().size() == expectedNumberOfRecords)
            .collect(toMap(e -> Instant.parse((String) e.getKey()), Map.Entry::getValue));
        assert fullBatchesOnly.size() > 0;
        return fullBatchesOnly.entrySet().stream().max(Map.Entry.comparingByKey()).get();
    }

    private static void logShardAllocationInformation(String indexName) throws IOException {
        var msg = prepareShardAllocationInformation(indexName);
        logger.info(msg);
    }

    private static String prepareShardAllocationInformation(String indexName) throws IOException {
        StringBuilder msgBuilder = new StringBuilder();
        Response nodeNamesResponse = client().performRequest(new Request("GET", "/_cat/nodes?h=name"));
        String nodeNames = EntityUtils.toString(nodeNamesResponse.getEntity());
        for (int shardNumber = 0; shardNumber < 3; shardNumber++) {
            for (String nodeName : nodeNames.split("\n")) {
                for (boolean isPrimary : new boolean[] { true, false }) {
                    try {
                        Request get = new Request("GET", "/_cluster/allocation/explain");
                        get.setJsonEntity(Strings.format("""
                            {
                              "index": "%s",
                              "shard" : %d,
                              "primary": %b,
                              "current_node": "%s"
                            }
                            """, indexName, shardNumber, isPrimary, nodeName));
                        Response response = client().performRequest(get);
                        msgBuilder.append((isPrimary ? "primary " : "replica ") + shardNumber);
                        msgBuilder.append(
                            XContentHelper.convertToJson(
                                BytesReference.fromByteBuffer(ByteBuffer.wrap(EntityUtils.toByteArray(response.getEntity()))),
                                true
                            )
                        );
                    } catch (ResponseException e) {

                    }
                }
            }
        }
        return msgBuilder.toString();
    }
}
