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

import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class StorageMeteringIT extends AbstractMeteringIntegTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    protected static final int DEFAULT_SEARCH_POWER = 100;
    private static final int ASCII_SIZE = 1;
    private static final int NUMBER_SIZE = Long.BYTES;
    private static int EXPECTED_SIZE = 10 * ASCII_SIZE + NUMBER_SIZE + 6 * ASCII_SIZE;

    Map<String, Object> expectedDefaultAttributes = Map.of(
        "boost_window",
        (int) DEFAULT_BOOST_WINDOW.seconds(),
        "search_power",
        DEFAULT_SEARCH_POWER
    );

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var list = new ArrayList<Class<? extends Plugin>>();
        list.addAll(super.nodePlugins());
        list.add(InternalSettingsPlugin.class);
        return list;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), DEFAULT_BOOST_WINDOW)
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), DEFAULT_SEARCH_POWER)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), DEFAULT_SEARCH_POWER)
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), ProjectType.OBSERVABILITY)
            .put(MeteringPlugin.NEW_IX_METRIC_SETTING.getKey(), "true")
            .put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false)
            .put(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();
    }

    @After
    public void cleanup() {
        receivedMetrics().clear();
        assertAcked(
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 1;
    }

    @TestLogging(reason = "development", value = "co.elastic.elasticsearch.metering:TRACE")
    public void testRaStorageIsReportedAfterCommit() throws InterruptedException, ExecutionException {
        String indexName = "idx1";
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
        createTimeSeriesIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWith(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", EXPECTED_SIZE);

        usageRecord = filterByIdStartsWith(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", EXPECTED_SIZE);
    }

    public void testRAStorageIsAccumulated() throws InterruptedException, ExecutionException {
        String indexName = "idx2";
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
        createTimeSeriesIndex(indexName);

        client().bulk(
            new BulkRequest().add(
                new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc"),
                new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "def")
            )
        ).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWith(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", 2 * EXPECTED_SIZE);

        usageRecord = filterByIdStartsWith(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", 2 * EXPECTED_SIZE);
    }

    private void createTimeSeriesIndex(String indexName) {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("key")).build();

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settings)
            .setMapping("@timestamp", "type=date", "key", "type=keyword,time_series_dimension=true")
            .get();
    }

    private static void assertUsageRecord(
        String indexName,
        UsageRecord metric,
        String expectedid,
        String expectedType,
        int expectedQuantity
    ) {
        assertThat(metric.id(), startsWith(expectedid));
        assertThat(metric.usage().type(), equalTo(expectedType));
        assertThat(metric.usage().quantity(), equalTo((long) expectedQuantity));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName)));
    }

}
