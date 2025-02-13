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
import static org.hamcrest.Matchers.hasSize;

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

        List<Map<?, ?>> latestIXShardSizes = new ArrayList<>();
        List<Map<?, ?>> latestIXIndexSizes = new ArrayList<>();
        assertBusy(() -> {
            var allUsageRecords = getAllUsageRecords();
            var ixShardSizeRecords = filterUsageRecords(allUsageRecords, "shard-size");
            var ixIndexSizeRecords = filterUsageRecords(allUsageRecords, "index-size");
            var raStorageRecords = filterUsageRecords(allUsageRecords, "raw-stored-index-size");

            // there might be records from multiple metering periods, we are interested in the latest only

            // we are expecting 1 record (1 per index)
            var latestRAStorageBatch = getLatestFullBatch(raStorageRecords, 1);
            assertThat(usageQuantity(latestRAStorageBatch.get(0)), equalTo(numDocs * rawSizePerDoc));

            // we are expecting 1 record (1 per index)
            latestIXIndexSizes.clear();
            latestIXIndexSizes.addAll(getLatestFullBatch(ixIndexSizeRecords, 1));

            // we are expecting 3 records (1 per shard)
            latestIXShardSizes.clear();
            latestIXShardSizes.addAll(getLatestFullBatch(ixShardSizeRecords, 3));
            logger.info(debugInfoForShardSize(INDEX_NAME, latestIXShardSizes, ixShardSizeRecords));
        }, 30, TimeUnit.SECONDS);

        // those are the expected values taken from the /_cat/segments api
        // we are asserting that there will be only 1 segment per shard and taking its sizeInBytes
        Map<Integer, Integer> expectedShardSizes = getExpectedReplicaShardSizes();
        int expectedIndexSize = expectedShardSizes.values().stream().mapToInt(Integer::intValue).sum();

        Map<String, List<Map<?, ?>>> indexSizesByNode = groupBySourceNodeName(latestIXIndexSizes);
        var sourceNode = indexSizesByNode.keySet().iterator().next();
        assertThat(indexSizesByNode.size(), equalTo(1));
        assertThat(indexSizesByNode.get(sourceNode), contains(transformedMatch(m -> usageQuantity(m), equalTo(expectedIndexSize))));

        Map<String, List<Map<?, ?>>> shardSizesByNode = groupBySourceNodeName(latestIXShardSizes);
        assertThat(shardSizesByNode.keySet(), equalTo(indexSizesByNode.keySet()));
        assertThat(shardSizesByNode.get(sourceNode), hasSize(3));
        assertThat(
            shardSizesByNode + " vs " + expectedShardSizes,
            shardNumberToSize(shardSizesByNode.get(sourceNode)),
            equalTo(expectedShardSizes)
        );
    }

    private void forceMerge() throws IOException {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
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
    private Map<Integer, Integer> getExpectedReplicaShardSizes() throws IOException {

        Map<String, Object> indices = entityAsMap(client().performRequest(new Request("GET", INDEX_NAME + "/_segments")));

        Map<String, List<Map<String, ?>>> shards = (Map<String, List<Map<String, ?>>>) XContentMapValues.extractValue(
            "indices." + INDEX_NAME + ".shards",
            indices
        );

        Map<Integer, Integer> shardNumberToSize = new HashMap<>();
        shards.forEach((shardNumber, shardCopies) -> {
            shardCopies.forEach(shardCopyInfo -> {
                // skipping primaries info
                if (((boolean) XContentMapValues.extractValue("routing.primary", shardCopyInfo)) == false) {
                    var segments = (Map<String, ?>) XContentMapValues.extractValue("segments", shardCopyInfo);
                    assert segments.size() == 1;// important, we expect segments to be merged to 1
                    var segment = (Map<String, ?>) segments.values().iterator().next();
                    shardNumberToSize.compute(Integer.parseInt(shardNumber), (shard, prevSize) -> {
                        var size = (Integer) segment.get("size_in_bytes");
                        assert prevSize == null || prevSize.equals(size)
                            : "Inconsistent replica size for shard " + shard + ": " + prevSize + " vs " + size;
                        return size;
                    });
                }
            });
        });
        return shardNumberToSize;
    }

    private String debugInfoForShardSize(String indexName, List<Map<?, ?>> latestRecords, List<Map<?, ?>> shardSizeRecords)
        throws IOException {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Latest timestamp: " + latestRecords.get(0).get("usage_timestamp"));
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append("Latest records: " + latestRecords);
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append("All usage records for shard-size: " + shardSizeRecords);
        msgBuilder.append(System.lineSeparator());
        msgBuilder.append(prepareShardAllocationInformation(indexName));
        return msgBuilder.toString();
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
