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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MeteringRestTestIT extends AbstractMeteringRestTestIT {
    private static final Logger logger = LogManager.getLogger(MeteringRestTestIT.class);

    @Override
    protected Settings restClientSettings() {
        return restAdminSettings();
    }

    public void testMeteringRecordsCanBeDeduplicated() throws Exception {
        // This test asserts that a duplicated metrics (due to multiple replicas) can be deduplicated.
        // metrics can only be deduplicated if their id is the same (including the timestamp)
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        createIndex(INDEX_NAME, settings);

        // ingest more docs so that each shard has some
        int numDocs = 99;
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

        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush"));

        logShardAllocationInformation(INDEX_NAME);

        assertBusy(() -> {
            var usageRecords = drainUsageRecords("shard-size");
            // there might be records from multiple metering.report_period. We are interested in the latest
            // because the replication might take time and also some nodes might be reporting with a delay

            Optional<Instant> timestamp = getLatestUsageTimestamp(usageRecords);
            if (timestamp.isPresent()) {
                List<Map<?, ?>> latestRecords = getRecordsForLatestTimestamp(usageRecords, timestamp);

                // there are 6 records. Each primary shard (3) has 2 replicas. This accounts to 6 replica shards.
                assertThat(latestRecords.size(), equalTo(6));
                Map<?, List<Map<?, ?>>> groupedById = latestRecords.stream().collect(groupingBy(m -> m.get("id")));
                // there are 3 primary shards, so should be 3 unique ids only
                assertThat(groupedById, equalTo(3));
                // each primary is duplicated (replicated) twice
                groupedById.values().forEach(l -> assertThat(l, hasSize(2)));
            }
        }, 30, TimeUnit.SECONDS);

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
