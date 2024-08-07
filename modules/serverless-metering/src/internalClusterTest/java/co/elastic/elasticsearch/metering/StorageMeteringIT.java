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

import co.elastic.elasticsearch.metering.codec.RAStorageDocValuesFormatFactory;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(
    reason = "development",
    value = "co.elastic.elasticsearch.metering:TRACE,co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageReporter:TRACE"
)
public class StorageMeteringIT extends AbstractMeteringIntegTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    protected static final int DEFAULT_SEARCH_POWER = 100;
    private static final int ASCII_SIZE = 1;
    private static final int NUMBER_SIZE = Long.BYTES;
    private static final long EXPECTED_SIZE = 10 * ASCII_SIZE + NUMBER_SIZE + 6 * ASCII_SIZE;

    /**
     * This extension of the Serverless plugin allow us to intercept and inject a different MergePolicy.
     * This is necessary to make merges more "deterministic" in tests; in particular, when we want to force a merge to
     * happen immediately and get rid of all deleted docs, including soft deletes.
     * The standard merge policy is more complex, as it includes retention leases which retain docs for peer-recovery purposes (see
     * {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy}). This classes make it possible to optionally bypass that,
     * by calling {@link CustomMergePolicyStatelessPlugin#enableCustomMergePolicy(MergePolicy)}
     */
    public static class CustomMergePolicyStatelessPlugin extends Stateless {
        private static class TestFilterMergePolicy extends FilterMergePolicy {
            TestFilterMergePolicy(MergePolicy in) {
                super(in);
            }

            @Override
            public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
                throws IOException {
                var mergePolicy = customMergePolicy.get();
                if (mergePolicy != null) {
                    return mergePolicy.findMerges(mergeTrigger, segmentInfos, mergeContext);
                } else {
                    return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
                }
            }

            @Override
            public MergeSpecification findForcedMerges(
                SegmentInfos segmentInfos,
                int maxSegmentCount,
                Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                MergeContext mergeContext
            ) throws IOException {
                var mergePolicy = customMergePolicy.get();
                if (mergePolicy != null) {
                    return mergePolicy.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
                } else {
                    return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
                }
            }

            @Override
            public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
                throws IOException {
                var mergePolicy = customMergePolicy.get();
                if (mergePolicy != null) {
                    return mergePolicy.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
                } else {
                    return super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
                }
            }

            @Override
            public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
                var mergePolicy = customMergePolicy.get();
                if (mergePolicy != null) {
                    return mergePolicy.findForcedDeletesMerges(segmentInfos, mergeContext);
                } else {
                    return super.findForcedDeletesMerges(segmentInfos, mergeContext);
                }
            }

            @Override
            public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
                var mergePolicy = customMergePolicy.get();
                if (mergePolicy != null) {
                    return mergePolicy.numDeletesToMerge(info, delCount, readerSupplier);
                }
                return super.numDeletesToMerge(info, delCount, readerSupplier);
            }
        }

        static final AtomicReference<MergePolicy> customMergePolicy = new AtomicReference<>(null);
        static final MergePolicy simpleMergePolicy = new TieredMergePolicy().setForceMergeDeletesPctAllowed(0).setDeletesPctAllowed(5);

        static void enableCustomMergePolicy(MergePolicy mergePolicy) {
            customMergePolicy.set(mergePolicy);
        }

        static void disableCustomMergePolicy() {
            customMergePolicy.set(null);
        }

        public CustomMergePolicyStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public void loadExtensions(ExtensionLoader loader) {
            this.codecWrapper = createCodecWrapper(new RAStorageDocValuesFormatFactory());
        }

        @Override
        protected MergePolicy getMergePolicy(EngineConfig engineConfig) {
            return new TestFilterMergePolicy(super.getMergePolicy(engineConfig));
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider,
            TranslogRecoveryMetrics translogRecoveryMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                translogRecoveryMetrics
            ) {
                @Override
                protected IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    iwc.setMergePolicy(engineConfig.getMergePolicy());
                    return super.createWriter(directory, iwc);
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(CustomMergePolicyStatelessPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(DataStreamsPlugin.class);
        plugins.add(TestScriptPlugin.class);
        return plugins;
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
        CustomMergePolicyStatelessPlugin.disableCustomMergePolicy();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 1;
    }

    public void testRaStorageFieldInaccessible() {
        String indexName = "idx1";
        createIndex(indexName);
        String id = client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar"))
            .actionGet()
            .getId();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        List<SearchSourceBuilder> searchBuilders = List.of(
            new SearchSourceBuilder().fetchField(RaStorageMetadataFieldMapper.FIELD_NAME).query(new MatchAllQueryBuilder()),
            new SearchSourceBuilder().query(new ExistsQueryBuilder(RaStorageMetadataFieldMapper.FIELD_NAME)),
            new SearchSourceBuilder().query(new TermQueryBuilder(RaStorageMetadataFieldMapper.FIELD_NAME, 0))
        );

        // can't query for it
        for (var source : searchBuilders) {
            Exception e = expectThrows(Exception.class, () -> client().search(new SearchRequest(indexName).source(source)).actionGet());
            assertThat(
                ElasticsearchException.guessRootCauses(e)[0].getMessage(),
                anyOf(
                    containsString("Cannot fetch values for internal field [_rastorage]"),
                    containsString("Cannot run exists query on [_rastorage]"),
                    containsString("The [_rastorage] field may not be queried directly")
                )
            );
        }

        // can't set it
        Exception e = expectThrows(
            Exception.class,
            () -> client().index(
                new IndexRequest(indexName).source(
                    XContentType.JSON,
                    "value1",
                    "foo",
                    "value2",
                    "bar",
                    RaStorageMetadataFieldMapper.FIELD_NAME,
                    100L
                )
            ).actionGet()
        );
        assertThat(
            ElasticsearchException.guessRootCauses(e)[0].getMessage(),
            containsString("Field [_rastorage] is a metadata field and cannot be added inside a document.")
        );

        // can't update it
        e = expectThrows(
            Exception.class,
            () -> client().update(new UpdateRequest(indexName, id).doc(RaStorageMetadataFieldMapper.FIELD_NAME, 100L)).actionGet()
        );
        assertThat(
            ElasticsearchException.guessRootCauses(e)[0].getMessage(),
            containsString("Field [_rastorage] is a metadata field and cannot be added inside a document.")
        );
    }

    public void testNonDataStreamWithTimestamp() throws InterruptedException {
        String indexName = "idx1";

        // document contains a @timestamp field but it is not a timeseries data stream (no mappings with that field created upfront)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", equalTo(EXPECTED_SIZE));

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", equalTo(EXPECTED_SIZE));
    }

    public void testRAStorageWithTimeSeries() throws InterruptedException {
        String indexName = "idx1";
        createTimeSeriesIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        waitUntil(() -> hasReceivedRecords("raw-stored-index-size:" + indexName));
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecordStream = pollReceivedRecords();
        UsageRecord usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "raw-stored-index-size:" + indexName);
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", equalTo(EXPECTED_SIZE));

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", equalTo(EXPECTED_SIZE));
    }

    public void testDataStreamNoMapping() throws InterruptedException, IOException {
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
        assertUsageRecord(dsName, usageRecord, "raw-stored-index-size:" + dsName, "es_raw_stored_data", equalTo(EXPECTED_SIZE));

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + dsName);
        assertUsageRecord(dsName, usageRecord, "ingested-doc:" + dsName, "es_raw_data", equalTo(EXPECTED_SIZE));
    }

    public void testRaStorageIsReportedAfterCommit() throws InterruptedException, IOException {
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
        assertUsageRecord(dsName, usageRecord, "raw-stored-index-size:" + dsName, "es_raw_stored_data", equalTo(EXPECTED_SIZE));

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + dsName);
        assertUsageRecord(dsName, usageRecord, "ingested-doc:" + dsName, "es_raw_data", equalTo(EXPECTED_SIZE));
    }

    // this test is confirming that for nontimeseries index we will meter ra-s updates by script in solution's cluster
    // if we didn't the ra-s would decrease after an update by script (because of a delete being followed by a not metered index op)
    public void testUpdatesViaScriptAreMeteredForSolutions() throws Exception {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName = "index1";

        createIndex(indexName);

        String scriptId = "script1";
        putJsonStoredScript(scriptId, Strings.format("""
            {"script": {"lang": "%s", "source": "ctx._source.b = 'xx'"} }""", MockScriptEngine.NAME));

        // combining an index and 2 updates and expecting only the metering value for the new indexed doc & partial update
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        long raSize = 3 * ASCII_SIZE + NUMBER_SIZE;

        // update via stored script
        final Script storedScript = new Script(ScriptType.STORED, null, scriptId, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(storedScript).get();

        // update via inlined script
        String scriptCode = "ctx._source.b = 'xxx'";
        final Script script = new Script(ScriptType.INLINE, TestScriptPlugin.NAME, scriptCode, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(script).get();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        List<UsageRecord> usageRecords = new ArrayList<>();
        assertBusy(() -> {
            usageRecords.addAll(pollReceivedRecords());
            var ingestRecords = usageRecords.stream().filter(m -> m.id().startsWith("ingested-doc:" + indexName)).toList();
            // we don't expect RA-I records for updates, hence only 1 record from the newDoc request
            assertThat(ingestRecords.size(), equalTo(1));

            assertThat(ingestRecords.stream().map(x -> x.usage().type()).toList(), everyItem(startsWith("es_raw_data")));
            assertThat(ingestRecords.stream().map(x -> x.source().metadata().get("index")).toList(), everyItem(startsWith(indexName)));

            var totalQuantity = ingestRecords.stream().mapToLong(x -> x.usage().quantity()).sum();
            assertThat(totalQuantity, equalTo(raSize));
        });

        waitAndAssertRAStorageRecords(usageRecords, indexName, raSize, 1);
        receivedMetrics().clear();
    }

    // this test is confirming that for nontimeseries index we will meter ra-s updates by script in solution's cluster
    // if we didn't the ra-s would decrease after an update by script (because of a delete being followed by a not metered index op)
    public void testUpdatesViaDocAreMeteredForSolutions() throws Exception {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName = "index1";

        createIndex(indexName);

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        long raSize = 3 * ASCII_SIZE + NUMBER_SIZE;
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        client().admin().indices().prepareFlush(indexName).get().getStatus().getStatus();

        List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, raSize);
        waitAndAssertRAStorageRecords(usageRecords, indexName, raSize, 1);

        usageRecords.clear();
        receivedMetrics().clear();

        long raUpdateSize = ASCII_SIZE + NUMBER_SIZE;
        client().prepareUpdate().setIndex(indexName).setId("1").setDoc(jsonBuilder().startObject().field("d", 2).endObject()).get();

        waitAndAssertRAIngestRecords(usageRecords, indexName, raUpdateSize);
        waitAndAssertRAStorageRecords(usageRecords, indexName, raSize + raUpdateSize, 1);

        receivedMetrics().clear();
    }

    public void testRAStorageIsAccumulated() throws InterruptedException {
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
        assertUsageRecord(indexName, usageRecord, "raw-stored-index-size:" + indexName, "es_raw_stored_data", equalTo(2 * EXPECTED_SIZE));

        usageRecord = filterByIdStartsWithAndGetFirst(usageRecordStream, "ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, "ingested-doc:" + indexName, "es_raw_data", equalTo(2 * EXPECTED_SIZE));
    }

    public void testRAStorageWithNonTimeSeries() throws Exception {
        String indexName = "idx1";
        createIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, EXPECTED_SIZE);
        waitAndAssertRAStorageRecords(usageRecords, indexName, EXPECTED_SIZE, 0);
    }

    public void testRAStorageWithNonTimeSeriesMultipleUniformDocs() throws Exception {
        String indexName = "idx1";
        createIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, 3 * EXPECTED_SIZE);
        waitAndAssertRAStorageRecords(usageRecords, indexName, 3 * EXPECTED_SIZE, 1);
    }

    public void testRAStorageWithNonTimeSeriesMultipleDifferentDocs() throws Exception {
        String indexName = "idx1";
        createIndex(indexName);

        var doc1RASize = 10 * ASCII_SIZE + NUMBER_SIZE + 6 * ASCII_SIZE;
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        var doc2RASize = "some_other_field".length() * ASCII_SIZE + 2 * NUMBER_SIZE + "key".length() * ASCII_SIZE;
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_other_field", 123, "key", 456)).actionGet();
        var doc3RASize = 3 * ASCII_SIZE + NUMBER_SIZE;
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 123, "b", "c")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();
        long raIngestedSize = doc1RASize + doc2RASize + doc3RASize;
        long raAvgPerDoc = raIngestedSize / 3;
        long raEstimated = raAvgPerDoc * 3;

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, raIngestedSize);
        waitAndAssertRAStorageRecords(usageRecords, indexName, raEstimated, 1);
    }

    public void testRAStorageWithNonTimeSeriesAndDeletesNoMerge() throws Exception {
        CustomMergePolicyStatelessPlugin.enableCustomMergePolicy(NoMergePolicy.INSTANCE);
        String indexName = "idx1";
        createIndex(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        var result = client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 123, "b", "c")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();
        client().delete(new DeleteRequest(indexName, result.getId())).actionGet();
        var raIngestedSize = 2 * EXPECTED_SIZE + (3 * ASCII_SIZE + NUMBER_SIZE);
        var raAvgPerDoc = raIngestedSize / 3;
        var raEstimated = raAvgPerDoc * 2;

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, raIngestedSize);
        waitAndAssertRAStorageRecords(usageRecords, indexName, raEstimated, 1);
    }

    public void testRAStorageWithNonTimeSeriesAndDeletesAndMerge() throws Exception {
        CustomMergePolicyStatelessPlugin.enableCustomMergePolicy(CustomMergePolicyStatelessPlugin.simpleMergePolicy);

        String indexName = "idx1";
        createIndex(indexName, indexSettings(1, 1).put(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), TimeValue.ZERO).build());
        ensureGreen(indexName);

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        var result1 = client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        var result2 = client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        client().delete(new DeleteRequest(indexName, result1.getId()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        client().delete(new DeleteRequest(indexName, result2.getId()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();

        admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, 3 * EXPECTED_SIZE);
        waitAndAssertRAStorageRecords(usageRecords, indexName, EXPECTED_SIZE, 0);
    }

    public void testRAStorageWithNonTimeSeriesAllDeletedAndMerge() throws Exception {
        CustomMergePolicyStatelessPlugin.enableCustomMergePolicy(CustomMergePolicyStatelessPlugin.simpleMergePolicy);

        String indexName = "idx1";
        createIndex(indexName, indexSettings(1, 1).put(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), TimeValue.ZERO).build());
        ensureGreen(indexName);

        var result1 = client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        var result2 = client().index(new IndexRequest(indexName).source(XContentType.JSON, "some_field", 123, "key", "abc")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        client().delete(new DeleteRequest(indexName, result1.getId()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        client().delete(new DeleteRequest(indexName, result2.getId()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1)).actionGet();

        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<UsageRecord> usageRecords = new ArrayList<>();
        waitAndAssertRAIngestRecords(usageRecords, indexName, 2 * EXPECTED_SIZE);
        waitAndAssertRAStorageRecords(usageRecords, indexName, 0, 0);
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
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).actionGet();
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
        Matcher<? super Long> matcher
    ) {
        assertThat(metric.id(), startsWith(expectedid));
        assertThat(metric.usage().type(), equalTo(expectedType));
        assertThat(metric.source().metadata().get("index"), startsWith(indexName));
        assertThat(metric.usage().quantity(), matcher);
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

    private void waitAndAssertRAStorageRecords(List<UsageRecord> usageRecords, String indexName, long raStorageSize, long delta)
        throws Exception {
        assertBusy(() -> {
            usageRecords.addAll(pollReceivedRecords());
            var lastUsageRecord = usageRecords.stream()
                .filter(m -> m.id().startsWith("raw-stored-index-size:" + indexName))
                .max(Comparator.comparing(UsageRecord::usageTimestamp));
            assertFalse(lastUsageRecord.isEmpty());
            assertUsageRecord(
                indexName,
                lastUsageRecord.get(),
                "raw-stored-index-size:" + indexName,
                "es_raw_stored_data",
                // +/-delta to account for approximation on averages
                both(greaterThanOrEqualTo(raStorageSize - delta)).and(lessThanOrEqualTo(raStorageSize + delta))
            );
        });
    }
}
