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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class MeteringVCURestTestIT extends AbstractMeteringRestTestIT {

    public void testMeteringRecords() throws Exception {
        var expectedMemory = expectedPhysicalMemory();

        // Create index with admin client to avoid tracking action
        createIndex(adminClient(), indexName, Settings.EMPTY);
        var afterIndexCreate = Instant.now();

        // Assert expected records before any activity is recorded
        assertBusy(() -> {
            var metrics = getActivityRecords();
            assertAlwaysSetFields(metrics, expectedMemory);

            var search = metrics.search().stream().filter(r -> r.creationTime().isAfter(afterIndexCreate)).toList();
            assertFalse(search.isEmpty());
            search.forEach(this::assertNonActive);
            search.forEach(this::assertSPMinProvisionedMemoryZero);

            var index = metrics.index().stream().filter(r -> r.creationTime().isAfter(afterIndexCreate)).toList();
            assertFalse(index.isEmpty());
            index.forEach(this::assertNonActive);
            index.forEach(this::assertSPMinProvisionedMemoryNotPresent);
        }, 30, TimeUnit.SECONDS);

        // Insert a doc, which is tracked as an index activity.
        var beforeBulk = Instant.now();
        addDoc(client());
        var afterBulk = Instant.now();
        ensureGreen(adminClient(), indexName);

        // Assert index activity but no search activity
        assertBusy(() -> {
            var metrics = getActivityRecords();
            assertAlwaysSetFields(metrics, expectedMemory);

            // After adding a doc index tier is active
            var index = metrics.index().stream().filter(VcuRecord::active).toList();
            assertFalse(index.isEmpty());
            index.forEach(r -> assertActive(r, beforeBulk, afterBulk));
            index.forEach(this::assertSPMinProvisionedMemoryNotPresent);

            // All search records should still be inactive
            var search = metrics.search().stream().toList();
            assertFalse(search.isEmpty());
            search.forEach(this::assertNonActive);
            search.forEach(this::assertSPMinProvisionedMemoryNonZero);
        }, 30, TimeUnit.SECONDS);

        // Run _search, which is tracked as search activity.
        var beforeSearch = Instant.now();
        client().performRequest(new Request("GET", "/" + indexName + "/_search"));
        var afterSearch = Instant.now();
        ensureGreen(adminClient(), indexName);

        // Assert search activity and no new index activity
        assertBusy(() -> {
            var metrics = getActivityRecords();
            assertAlwaysSetFields(metrics, expectedMemory);

            // After search request, search tier is active
            var search = metrics.search().stream().filter(VcuRecord::active).toList();
            assertFalse(search.isEmpty());
            search.forEach(r -> assertActive(r, beforeSearch, afterSearch));
            search.forEach(this::assertSPMinProvisionedMemoryNonZero);

            // Index tier is still active, but latest timestamp is from previous activity.
            var index = metrics.index().stream().filter(VcuRecord::active).toList();
            assertFalse(index.isEmpty());
            index.forEach(r -> assertActive(r, beforeBulk, afterBulk));
            index.forEach(this::assertSPMinProvisionedMemoryNotPresent);
        }, 30, TimeUnit.SECONDS);

        // Run refresh request
        var beforeRefresh = Instant.now();
        client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
        var afterRefresh = Instant.now();
        ensureGreen(adminClient(), indexName);

        // Assert both search and index activity
        assertBusy(() -> {
            var metrics = getActivityRecords();
            assertAlwaysSetFields(metrics, expectedMemory);

            assertFalse(metrics.search().isEmpty());
            metrics.search().forEach(r -> assertActive(r, beforeRefresh, afterRefresh));
            metrics.search().forEach(this::assertSPMinProvisionedMemoryNonZero);

            assertFalse(metrics.index().isEmpty());
            metrics.index().forEach(r -> assertActive(r, beforeRefresh, afterRefresh));
            metrics.index().forEach(this::assertSPMinProvisionedMemoryNotPresent);
        }, 30, TimeUnit.SECONDS);
    }

    record Metrics(List<VcuRecord> search, List<VcuRecord> index) {};

    private Metrics getActivityRecords() {
        var usageRecords = usageApiTestServer.drainAllUsageRecords();
        var search = UsageApiTestServer.filterUsageRecords(usageRecords, "vcu:search").stream().map(VcuRecord::fromRecord).toList();
        var index = UsageApiTestServer.filterUsageRecords(usageRecords, "vcu:index").stream().map(VcuRecord::fromRecord).toList();
        return new Metrics(search, index);
    }

    record VcuRecord(
        Instant creationTime,
        String type,
        long quantity,
        boolean active,
        String applicationTier,
        Instant latestActivityTimestamp,
        Long spMinProvisionedMemory
    ) {
        static VcuRecord fromRecord(Map<?, ?> record) {
            var latestActivity = (String) extractValue("usage.metadata.latest_activity_timestamp", record);
            var spMinProvisionedMemory = (String) extractValue("usage.metadata.sp_min_provisioned_memory", record);
            return new VcuRecord(
                Instant.parse((String) extractValue("creation_timestamp", record)),
                (String) extractValue("usage.type", record),
                (long) extractValue("usage.quantity", record),
                Boolean.parseBoolean((String) extractValue("usage.metadata.active", record)),
                (String) extractValue("usage.metadata.application_tier", record),
                latestActivity == null ? null : Instant.parse(latestActivity),
                spMinProvisionedMemory == null ? null : Long.parseLong(spMinProvisionedMemory)
            );
        }
    };

    private void assertActive(VcuRecord record, Instant beforeActivity, Instant afterActivity) {
        assertThat(record.active(), equalTo(true));
        assertTrue(record.latestActivityTimestamp().isAfter(beforeActivity));
        assertTrue(record.latestActivityTimestamp().isBefore(afterActivity));
    }

    private void assertNonActive(VcuRecord record) {
        assertThat(record.active(), equalTo(false));
        assertThat(record.latestActivityTimestamp(), nullValue());
    }

    private void assertAlwaysSetFieldsTier(VcuRecord record, String tier, long memory) {
        assertThat(record.type(), equalTo("es_vcu"));
        assertThat(record.quantity(), equalTo(memory));
        assertThat(record.applicationTier(), equalTo(tier));
    }

    private void assertSPMinProvisionedMemoryNotPresent(VcuRecord record) {
        assertThat(record.spMinProvisionedMemory(), nullValue());
    }

    private void assertSPMinProvisionedMemoryZero(VcuRecord record) {
        assertThat(record.spMinProvisionedMemory(), equalTo(0L));
    }

    private void assertSPMinProvisionedMemoryNonZero(VcuRecord record) {
        assertThat(record.spMinProvisionedMemory(), greaterThan(0L));
    }

    private void assertAlwaysSetFields(Metrics metrics, ExpectedMemory expectedMemory) {
        metrics.index().forEach(r -> assertAlwaysSetFieldsTier(r, "index", expectedMemory.index()));
        metrics.search().forEach(r -> assertAlwaysSetFieldsTier(r, "search", expectedMemory.search()));
    }

    private void addDoc(RestClient client) throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{}}\n");
        bulk.append("{\"foo\": \"bar\"}\n");
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        // Do not set refresh, as refresh is both a search and index action
        bulkRequest.setJsonEntity(bulk.toString());
        client.performRequest(bulkRequest);
    }

    record ExpectedMemory(long search, long index) {};

    private static ExpectedMemory expectedPhysicalMemory() throws IOException {
        // Make map from node id to node name, which contain search/index prefix
        var nodeNamesResponse = adminClient().performRequest(new Request("GET", "/_cat/nodes?full_id=true&h=id,name"));
        String nodeIdNames = EntityUtils.toString(nodeNamesResponse.getEntity());
        Map<String, String> nameToId = Arrays.stream(nodeIdNames.split("\n"))
            .map(s -> s.split(" "))
            .collect(Collectors.toMap(p -> p[1], p -> p[0]));

        var nodeStatsResponse = adminClient().performRequest(
            new Request("GET", "_nodes/stats/os?filter_path=nodes.*.os.mem.total_in_bytes")
        );
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            nodeStatsResponse.getEntity().getContent(),
            false
        );

        long indexTotal = 0;
        long searchTotal = 0;
        for (var nodeName : nameToId.keySet()) {
            var nodeId = nameToId.get(nodeName);
            var totalMem = (Long) extractValue("nodes." + nodeId + ".os.mem.total_in_bytes", responseMap);
            if (nodeName.startsWith("search")) {
                searchTotal += totalMem;
            } else {
                indexTotal += totalMem;
            }
        }
        return new ExpectedMemory(searchTotal, indexTotal);
    }
}
