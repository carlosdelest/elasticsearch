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

package co.elastic.elasticsearch.metering.usagereports;

import co.elastic.elasticsearch.metering.AbstractMeteringIntegTestCase;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskExecutor;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class UsageReportIT extends AbstractMeteringIntegTestCase {
    private static final String indexName = "idx1";

    @Before
    public void initClusterAndTestIndex() throws Exception {
        var internalCluster = internalCluster();

        startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();

        createTestIndex();

        int numberOfNodes = 3;
        ensureStableCluster(numberOfNodes);

        var initialMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster.clusterService().state());
        assertThat(initialMetadata, is(nullValue()));

        // Defer service start until we are ready to get "immediately" a good (stable) read
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    private void createTestIndex() {
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();
    }

    public void testLatestCommitedTimestampAdvancedWhenMetricRecordsSent() throws Exception {
        List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertIXRecords(usageRecords, indexName);

        var lastUsageTimestamp = getLastIXUsageTimestamp(usageRecords);

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(lastUsageTimestamp));
        });
    }

    public void testLatestCommitedTimestampPreservedWhenPersistentTaskNodeChange() throws Exception {
        final AtomicReference<Instant> currentCursor = new AtomicReference<>();

        List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertIXRecords(usageRecords, indexName);
        var lastUsageTimestamp = getLastIXUsageTimestamp(usageRecords);

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(lastUsageTimestamp));
            currentCursor.set(sampledMetricsMetadata.getCommittedTimestamp());
        });

        // toggle the persistent task executor, so we can check that metadata is preserved and picked up
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNull(task);
        });

        var afterStopMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
        assertThat(afterStopMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(currentCursor.get()));

        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });

        List<UsageRecord> newMetrics = new ArrayList<>();
        waitAndAssertIXRecords(newMetrics, indexName);
        var newLastUsageTimestamp = getLastIXUsageTimestamp(newMetrics);

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(newLastUsageTimestamp));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThan(currentCursor.get()));
        });
    }

    public void testRecordsSentFromLatestCommitedTimestampWhenPersistentTaskNodeRestarts() throws Exception {
        // Wait for metric records to be transmitted
        final AtomicReference<Instant> currentCursor = new AtomicReference<>();
        final AtomicReference<String> currentPersistentTaskNode = new AtomicReference<>();
        final AtomicBoolean samePersistentTaskNode = new AtomicBoolean();

        List<UsageRecord> metrics = new ArrayList<>();
        waitAndAssertIXRecords(metrics, indexName);
        var lastUsageTimestamp = getLastIXUsageTimestamp(metrics);

        // Compare them with cursor, remember it
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(clusterState);
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            var committedTimestamp = sampledMetricsMetadata.getCommittedTimestamp();
            assertThat(committedTimestamp, greaterThanOrEqualTo(lastUsageTimestamp));
            logger.info("Before restart committedTimestamp: [{}]", committedTimestamp);
            currentCursor.set(committedTimestamp);
            currentPersistentTaskNode.set(task.getExecutorNode());
        });

        // Switch off the persistent task executor, so we can check that metadata is preserved and picked up
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNull(task);
        });

        var afterStopMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
        assertThat(afterStopMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(currentCursor.get()));
        logger.info("After stop committedTimestamp: [{}]", afterStopMetadata.getCommittedTimestamp());

        // Provoke usage records to be generated
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        List<UsageRecord> newMetrics = new ArrayList<>();
        waitAndAssertRAIngestRecords(newMetrics, indexName);

        // Re-enable the persistent task
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
            var previousPersistentTaskNode = currentPersistentTaskNode.get();
            samePersistentTaskNode.set(previousPersistentTaskNode.equals(task.getExecutorNode()));
        });

        // If the new node is different and does not have previous information for interpolating, we will transmit just one sample
        var minimumSize = samePersistentTaskNode.get() ? 2 : 1;

        // Wait for new sampled metrics records
        var timestamps = new TreeSet<>(Instant::compareTo);
        assertBusy(() -> {
            assertTrue(hasReceivedRecords("shard-size"));
            waitAndAssertIXRecords(newMetrics, indexName);
            var newTimestamps = newMetrics.stream()
                .filter(m -> m.id().startsWith("shard-size"))
                .map(UsageRecord::usageTimestamp)
                .collect(Collectors.toSet());
            timestamps.addAll(newTimestamps);
            // Check we are sending records for the period we missed too
            assertThat(timestamps, hasSize(greaterThanOrEqualTo(minimumSize)));
        });

        if (samePersistentTaskNode.get()) {
            logger.info(
                "Same persistent task node, backfilling. Last committed: [{}], timestamps: [{}]",
                afterStopMetadata.getCommittedTimestamp(),
                timestamps.stream().map(Instant::toString).collect(Collectors.joining(";"))
            );
            // We expect to be able to backfill, so no holes
            Instant prevTimestamp = null;
            for (var timestamp : timestamps) {
                if (prevTimestamp != null) {
                    var difference = Duration.between(prevTimestamp, timestamp);
                    assertThat(difference.toMillis(), equalTo(REPORT_PERIOD.getMillis()));
                }
                prevTimestamp = timestamp;
            }
            // And no missing timestamp
            assertThat(timestamps, hasItem(afterStopMetadata.getCommittedTimestamp().plusMillis(REPORT_PERIOD.getMillis())));
        } else {
            logger.info(
                "Different persistent task node, dropping. Last committed: [{}], timestamps: [{}]",
                afterStopMetadata.getCommittedTimestamp(),
                timestamps.stream().map(Instant::toString).collect(Collectors.joining(";"))
            );
        }

        // Cursor advanced
        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            var committedTimestamp = sampledMetricsMetadata.getCommittedTimestamp();
            assertThat(committedTimestamp, greaterThanOrEqualTo(timestamps.last()));
            assertThat(committedTimestamp, greaterThan(currentCursor.get()));
        });
    }

    private void waitAndAssertIXRecords(List<UsageRecord> usageRecords, String indexName) throws Exception {
        assertBusy(() -> {
            pollReceivedRecords(usageRecords);
            var ixRecords = usageRecords.stream().filter(m -> m.id().startsWith("shard-size")).toList();
            assertFalse(ixRecords.isEmpty());

            assertThat(ixRecords.stream().map(UsageRecord::id).toList(), everyItem(startsWith("shard-size:" + indexName)));
            assertThat(ixRecords.stream().map(x -> x.usage().type()).toList(), everyItem(startsWith("es_indexed_data")));
            assertThat(ixRecords.stream().map(x -> x.source().metadata().get("index")).toList(), everyItem(startsWith(indexName)));
        });
    }

    private void waitAndAssertRAIngestRecords(List<UsageRecord> usageRecords, String indexName) throws Exception {
        assertBusy(() -> {
            pollReceivedRecords(usageRecords);
            var ingestRecords = usageRecords.stream().filter(m -> m.id().startsWith("ingested-doc:" + indexName)).toList();
            assertFalse(ingestRecords.isEmpty());

            assertThat(ingestRecords.stream().map(x -> x.usage().type()).toList(), everyItem(startsWith("es_raw_data")));
            assertThat(ingestRecords.stream().map(x -> x.source().metadata().get("index")).toList(), everyItem(startsWith(indexName)));
        });
    }

    private static Instant getLastIXUsageTimestamp(List<UsageRecord> usageRecords) {
        return usageRecords.stream()
            .filter(m -> m.id().startsWith("shard-size"))
            .map(UsageRecord::usageTimestamp)
            .max(Instant::compareTo)
            .orElseThrow(() -> new AssertionError("No IX usage records found"));
    }
}
