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

import co.elastic.elasticsearch.metering.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SampledMetricsMetadataServiceIT extends AbstractMeteringIntegTestCase {

    private static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    private static final int DEFAULT_SEARCH_POWER = 200;

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
        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });
    }

    @After
    public void cleanUp() {
        updateClusterSettings(
            Settings.builder()
                .putNull(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey())
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), DEFAULT_BOOST_WINDOW)
            .put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), DEFAULT_SEARCH_POWER)
            .put(MeteringPlugin.NEW_IX_METRIC_SETTING.getKey(), true)
            .put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false)
            .put(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(5))
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
        assertBusy(() -> assertTrue(hasReceivedRecords("shard-size")));

        List<UsageRecord> metrics = pollReceivedRecords("shard-size");
        var metric = metrics.get(0);

        String idPRefix = "shard-size:" + indexName;
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));

        var lastUsageTimestamp = metrics.stream().map(UsageRecord::usageTimestamp).max(Instant::compareTo).get();

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(lastUsageTimestamp));
        });
    }

    public void testLatestCommitedTimestampPreservedWhenPersistentTaskNodeChange() throws Exception {
        final AtomicReference<Instant> currentCursor = new AtomicReference<>();
        assertBusy(() -> assertTrue(hasReceivedRecords("shard-size")));

        List<UsageRecord> metrics = pollReceivedRecords("shard-size");
        var lastUsageTimestamp = metrics.stream().map(UsageRecord::usageTimestamp).max(Instant::compareTo).get();

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(lastUsageTimestamp));
            currentCursor.set(sampledMetricsMetadata.getCommittedTimestamp());
        });

        // toggle the persistent task executor, so we can check that metadata is preserved and picked up
        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNull(task);
        });

        var afterStopMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
        assertThat(afterStopMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(currentCursor.get()));

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });

        assertBusy(() -> assertTrue(hasReceivedRecords("shard-size")));
        List<UsageRecord> newMetrics = pollReceivedRecords("shard-size");
        var newLastUsageTimestamp = newMetrics.stream().map(UsageRecord::usageTimestamp).max(Instant::compareTo).get();

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
        assertBusy(() -> assertTrue(hasReceivedRecords("shard-size")));
        List<UsageRecord> metrics = pollReceivedRecords("shard-size");
        var lastUsageTimestamp = metrics.stream().map(UsageRecord::usageTimestamp).max(Instant::compareTo).get();

        // Compare them with cursor, remember it
        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(lastUsageTimestamp));
            currentCursor.set(sampledMetricsMetadata.getCommittedTimestamp());
        });

        // Switch off the persistent task executor, so we can check that metadata is preserved and picked up
        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNull(task);
        });

        var afterStopMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
        assertThat(afterStopMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(currentCursor.get()));

        // Provoke usage records to be generated
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        waitUntil(() -> pollReceivedRecords("ingested-doc:" + indexName).isEmpty() == false);

        // Check that non-sampled records did not advance the sampled metrics cursor
        var afterIngestMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
        assertThat(afterStopMetadata.getCommittedTimestamp(), equalTo(afterIngestMetadata.getCommittedTimestamp()));

        // Re-enable the persistent task
        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });

        // Wait for new sampled metrics records
        assertBusy(() -> assertTrue(hasReceivedRecords("shard-size")));
        List<UsageRecord> newMetrics = pollReceivedRecords("shard-size");

        // Check we are sending records for the period we missed too
        var timestamps = newMetrics.stream().map(UsageRecord::usageTimestamp).toList();
        assertThat(timestamps, hasSize(greaterThanOrEqualTo(2)));
        var newLastUsageTimestamp = timestamps.stream().max(Instant::compareTo).get();

        assertBusy(() -> {
            var sampledMetricsMetadata = SampledMetricsMetadata.getFromClusterState(internalCluster().clusterService().state());
            assertThat(sampledMetricsMetadata, is(notNullValue()));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThanOrEqualTo(newLastUsageTimestamp));
            assertThat(sampledMetricsMetadata.getCommittedTimestamp(), greaterThan(currentCursor.get()));
        });
    }
}
