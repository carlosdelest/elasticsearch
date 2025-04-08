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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class Metering1kDocsRestTestIT extends AbstractMeteringRestTestIT {

    private static final Logger logger = LogManager.getLogger(MeteringRestTestIT.class);

    @Override
    protected String projectType() {
        return randomFrom("SECURITY", "OBSERVABILITY");
    }

    @Override
    protected Settings restClientSettings() {
        return restAdminSettings();
    }

    public void testMeteringRecordsIn1kBatch() throws Exception {
        // This test asserts the ingested doc metric for 1k documents sums up to consistent value
        // this test also asserts about an exact value of index-size metrics. To make sure, that assertion
        // is consistent a shard has to be merged to a 1 segment. Then a value from /_cat/segments
        // is used as an expected value
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        createIndex(INDEX_NAME, settings);

        // ingest more docs so that each shard has some
        int numDocs = 1000;
        int rawSizePerDoc = 3; // raw size in bytes of the single doc

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        ensureGreen(INDEX_NAME);

        forceMerge();

        logShardAllocationInformation(INDEX_NAME);

        List<Map<?, ?>> ingestedDocs = new ArrayList<>();
        assertBusy(() -> {
            // ingested-doc metrics are emitted only once.
            // we need to await all are sent.
            var records = drainUsageRecords("ingested-doc");
            ingestedDocs.addAll(records);
            int sum = sumQuantity(ingestedDocs);

            // asserting that eventually records value sum up to expected value
            assertThat(sum, equalTo(numDocs * rawSizePerDoc));
            logger.info(numDocs);
            logger.info(ingestedDocs.size());
            logger.info(sum);
        });
        assertBusy(() -> {
            var allUsageRecords = drainAllUsageRecords();
            var ingestedDocsRecords = filterUsageRecords(allUsageRecords, "ingested-doc");
            // once we asserted the expected total ingest size there will be no more ingest usage records
            assertThat(ingestedDocsRecords, empty());
        });

        List<Map<?, ?>> latestIXIndexSizes = new ArrayList<>();
        assertBusy(() -> {
            var allUsageRecords = getAllUsageRecords();
            var ixIndexSizeRecords = filterUsageRecords(allUsageRecords, "index-size");
            var rawStorageRecords = filterUsageRecords(allUsageRecords, "raw-stored-index-size");

            // there might be records from multiple metering periods, we are interested in the latest only

            // we are expecting 1 record (1 per index)
            var latestRawStorageBatch = getLatestFullBatch(rawStorageRecords, 1);
            assertThat(usageQuantity(latestRawStorageBatch.get(0)), equalTo(numDocs * rawSizePerDoc));

            // we are expecting 1 record (1 per index)
            latestIXIndexSizes.clear();
            latestIXIndexSizes.addAll(getLatestFullBatch(ixIndexSizeRecords, 1));
        }, 30, TimeUnit.SECONDS);

        int expectedIndexSize = getExpectedIndexSize();

        Map<String, List<Map<?, ?>>> indexSizesByNode = groupBySourceNodeName(latestIXIndexSizes);
        var sourceNode = indexSizesByNode.keySet().iterator().next();
        assertThat(indexSizesByNode.size(), equalTo(1));
        assertThat(indexSizesByNode.get(sourceNode), contains(transformedMatch(m -> usageQuantity(m), equalTo(expectedIndexSize))));
    }

    private void forceMerge() throws IOException {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        request.addParameter("flush", "true");
        client().performRequest(request);
    }

    private Map<String, List<Map<?, ?>>> groupBySourceNodeName(List<Map<?, ?>> latestRecords) {
        Map<String, List<Map<?, ?>>> usagesByNodeName = new HashMap<>();
        latestRecords.forEach(record -> {
            String nodeName = (String) XContentMapValues.extractValue("source.id", record);
            List<Map<?, ?>> recordsForNodeName = usagesByNodeName.computeIfAbsent(nodeName, k -> new ArrayList<>());
            recordsForNodeName.add(record);
        });
        return usagesByNodeName;
    }

    @SuppressWarnings("unchecked")
    private int getExpectedIndexSize() throws IOException {
        Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", INDEX_NAME + "/_stats")));
        return (int) XContentMapValues.extractValue("indices." + INDEX_NAME + ".primaries.docs.total_size_in_bytes", stats);
    }

    private static int sumQuantity(List<Map<?, ?>> records) {
        return records.stream().mapToInt(Metering1kDocsRestTestIT::usageQuantity).sum();
    }

    private static int usageQuantity(Map<?, ?> record) {
        return (int) ((Map<?, ?>) record.get("usage")).get("quantity");
    }

    private static List<Map<?, ?>> getLatestFullBatch(List<Map<?, ?>> metric, int expectedNumberOfRecords) {
        Map<?, List<Map<?, ?>>> groupedByTimestamp = metric.stream().collect(groupingBy(m -> m.get("usage_timestamp")));
        Map<Instant, List<Map<?, ?>>> fullBatchesOnly = groupedByTimestamp.entrySet()
            .stream()
            .filter(e -> e.getValue().size() == expectedNumberOfRecords)
            .collect(toMap(e -> Instant.parse((String) e.getKey()), Map.Entry::getValue));
        assert fullBatchesOnly.size() > 0;
        return fullBatchesOnly.entrySet().stream().max(Map.Entry.comparingByKey()).get().getValue();
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
