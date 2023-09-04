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
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.test.cluster.serverless.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class Metering1kDocsRestTestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(MeteringRestTestIT.class);

    static int REPORT_PERIOD = 5;

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
        // this test also asserts about an approximate value of index-size metrics.
        // the exact value is not possible to be asserted as shards might be forcemerged at different time
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        String indexName = "test_index_1";
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

        client().performRequest(new Request("POST", "/" + indexName + "/_flush"));
        client().performRequest(new Request("POST", "/" + indexName + "/_forcemerge"));

        logShardAllocationInformation(indexName);

        List<Map<?, ?>> ingestedDocs = new ArrayList<>();
        assertBusy(() -> {
            // ingested-doc metrics are emitted only once.
            // we need to await all are sent.
            var records = usageApiTestServer.getUsageRecords("ingested-doc");
            ingestedDocs.addAll(records);
            int sum = sumQuantity(ingestedDocs);

            // asserting that eventually records value sum up to expected value
            assertThat(sum, equalTo(numDocs * 96 /* size of the single doc*/));
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

            Optional<Instant> timestamp = getLatestUsageTimestamp(shardSizeRecords);
            assert (timestamp.isPresent());
            List<Map<?, ?>> latestRecords = getRecordsForLatestTimestamp(shardSizeRecords, timestamp);
            // there are 6 records. Each primary shard (3) has 2 replicas. This accounts to 6 replica shards.
            assertThat(latestRecords.size(), equalTo(6));

            Map<?, List<Map<?, ?>>> groupedById = latestRecords.stream().collect(groupingBy(m -> m.get("id")));
            // there are 3 primary shards, so should be 3 unique ids only
            assertThat(groupedById.size(), equalTo(3));
            // each primary is duplicated (replicated) twice
            assertSumOnReplica(groupedById, 0);
            assertSumOnReplica(groupedById, 1);
        }, 30, TimeUnit.SECONDS);

    }

    private static void assertSumOnReplica(Map<?, List<Map<?, ?>>> groupedById, int replicaNumber) {
        List<Map<?, ?>> recordsOnReplica1 = groupedById.entrySet()
            .stream()
            .map(e -> e.getValue().get(replicaNumber))
            .collect(Collectors.toList());
        int sum = sumQuantity(recordsOnReplica1);
        int error = 1000;// TODO what should be a safe value? test runs were around 42xxx
        assertThat(sum, greaterThan(42252 - error));
        assertThat(sum, lessThan(42252 + error));
    }

    private static int sumQuantity(List<Map<?, ?>> ingestedDocs) {
        return ingestedDocs.stream().mapToInt(m -> (Integer) ((Map<?, ?>) m.get("usage")).get("quantity")).sum();
    }

    private static List<Map<?, ?>> getRecordsForLatestTimestamp(List<Map<?, ?>> metric, Optional<Instant> timestamp) {
        return metric.stream().filter(m -> m.get("usage_timestamp").equals(timestamp.get().toString())).collect(Collectors.toList());
    }

    private static Optional<Instant> getLatestUsageTimestamp(List<Map<?, ?>> metric) {
        return metric.stream()
            .map(m -> m.get("usage_timestamp"))
            .map(t -> Instant.parse((String) t))
            .sorted(Comparator.reverseOrder())
            .findFirst();
    }

    private static void logShardAllocationInformation(String indexName) throws IOException {
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
                        logger.info((isPrimary ? "primary " : "replica ") + shardNumber);
                        logger.info(
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
    }

}
