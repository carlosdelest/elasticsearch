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

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(
    reason = "development",
    value = "co.elastic.elasticsearch.metering:TRACE,co.elastic.elasticsearch.metering.ingested_size.RAStorageReporter:TRACE"
)
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
        list.add(DataStreamsPlugin.class);
        list.add(TestScriptPlugin.class);
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
            .put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false)
            .put(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();
    }

    @Before
    public void init() {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
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

    public void testNonDataStreamWithTimestamp() throws InterruptedException, ExecutionException {
        String indexName = "idx1";

        // document contains a @timestamp field but it is not a timeseries data stream (no mappings with that field created upfront)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", 0);

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", EXPECTED_SIZE);
    }

    public void testRAStorageWithTimeSeries() throws InterruptedException, ExecutionException, IOException {
        String indexName = "idx1";
        createTimeSeriesIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", EXPECTED_SIZE);

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", EXPECTED_SIZE);
    }

    public void testDataStreamNoMapping() throws InterruptedException, ExecutionException, IOException {
        String indexName = "idx1";
        String dsName = ".ds-" + indexName;

        String mapping = emptyMapping();
        createDataStreamAndTemplate(indexName, mapping);

        client().index(
            new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc").opType(DocWriteRequest.OpType.CREATE)
        ).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + dsName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + dsName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + dsName);
        assertUsageRecord(dsName, usageRecord, "raw-stored-index-size:" + dsName, "es_raw_stored_data", EXPECTED_SIZE);

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + dsName);
        assertUsageRecord(dsName, usageRecord, "ingested-doc:" + dsName, "es_raw_data", EXPECTED_SIZE);
    }

    public void testRaStorageIsReportedAfterCommit() throws InterruptedException, ExecutionException, IOException {
        String indexName = "idx1";
        String dsName = ".ds-" + indexName;
        createDataStream(indexName);

        client().index(
            new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc").opType(DocWriteRequest.OpType.CREATE)
        ).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + dsName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + dsName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + dsName);
        assertUsageRecord(dsName, usageRecord, "raw-stored-index-size:" + dsName, "es_raw_stored_data", EXPECTED_SIZE);

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + dsName);
        assertUsageRecord(dsName, usageRecord, "ingested-doc:" + dsName, "es_raw_data", EXPECTED_SIZE);
    }

    // this test is confirming that for nontimeseries index we will meter ra-s updates by script in solution's cluster
    // if we didn't the ra-s would decrease after an update by script (because of a delete being followed by a not metered index op)
    public void testUpdatesViaScriptAreMeteredForSolutions() throws Exception {
        // TODO remove this logging based assertion in favour of real assertions on metrics once they are emitted
        String loggerName = "co.elastic.elasticsearch.metering.ingested_size.RAStorageReporter";
        try (var mockLogAppender = MockLog.capture(loggerName)) {
            mockLogAppender.addExpectation(
                new MockLog.SeenEventExpectation(
                    "An RAStorageReported onParsingCompleted has been invoked",
                    loggerName,
                    Level.TRACE,
                    "parsing completed for non time series index"
                )
            );

            doTestUpdatesViaScriptAreMeteredForSolutions();

            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    private void doTestUpdatesViaScriptAreMeteredForSolutions() throws Exception {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName = "index1";

        createIndex(indexName);

        String scriptId = "script1";
        clusterAdmin().preparePutStoredScript().setId(scriptId).setContent(new BytesArray(Strings.format("""
            {"script": {"lang": "%s", "source": "ctx._source.b = 'xx'"} }""", MockScriptEngine.NAME)), XContentType.JSON).get();

        // combining an index and 2 updates and expecting only the metering value for the new indexed doc & partial update
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        long raIngestedSize = 3 * ASCII_SIZE + NUMBER_SIZE;

        // update via stored script
        final Script storedScript = new Script(ScriptType.STORED, null, scriptId, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(storedScript).get();

        // update via inlined script
        String scriptCode = "ctx._source.b = 'xx'";
        final Script script = new Script(ScriptType.INLINE, TestScriptPlugin.NAME, scriptCode, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(script).get();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, raIngestedSize);

        // we don't expect RA-I records for updates, hence only 1 record from the newDoc request
        long ingestRecordCount = usageRecords.stream().filter(m -> m.id().startsWith("ingested-doc")).count();
        assertThat(ingestRecordCount, equalTo(1L));

        // TODO assertions about RA-S for non timeseries
        receivedMetrics().clear();
    }

    private void createDataStream(String indexName) throws IOException {
        String mapping = mappingWithTimestamp();
        createDataStreamAndTemplate(indexName, mapping);
    }

    private static String emptyMapping() {
        return """
            {
                  "properties": {
                 }
            }""";
    }

    private static String mappingWithTimestamp() {
        return """
            {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    }
                 }
            }""";
    }

    protected static void createDataStreamAndTemplate(String dataStreamName, String mapping) throws IOException {
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(Collections.singletonList(dataStreamName))
                    .template(new Template(null, new CompressedXContent(mapping), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
        ).actionGet();
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).actionGet();
    }

    public void testRAStorageIsAccumulated() throws InterruptedException, ExecutionException {
        String indexName = "idx2";
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
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", 2 * EXPECTED_SIZE);

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
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
        assertThat(metric.source().metadata().get("index"), startsWith(indexName));
    }

    private void waitAndAssertRAIngestRecords(List<UsageRecord> usageRecords, String indexName, long raIngestedSize) throws Exception {
        assertBusy(() -> {
            usageRecords.addAll(pollReceivedRecords());
            var ingestRecords = usageRecords.stream().filter(m -> m.id().startsWith("ingested-doc:" + indexName)).toList();
            assertFalse(ingestRecords.isEmpty());

            assertThat(ingestRecords.stream().map(x -> x.usage().type()).toList(), everyItem(startsWith("es_raw_data")));
            assertThat(ingestRecords.stream().map(x -> x.source().metadata().get("index")).toList(), everyItem(startsWith(indexName)));

            var totalQuantity = ingestRecords.stream().mapToLong(x -> x.usage().quantity()).sum();
            assertThat(totalQuantity, equalTo(raIngestedSize));
        });
    }
}
