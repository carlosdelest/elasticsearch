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

package co.elastic.elasticsearch.stateless.engine.translog;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator.FLUSH_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessTranslogIT extends AbstractStatelessIntegTestCase {

    public void testTranslogFileHoldDirectoryOfReferencedFiles() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
    }

    public void testTranslogFileHoldDirectoryForIdleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        final String idleIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            idleIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ShardId idleShardId = new ShardId(resolveIndex(idleIndex), 0);

        ensureGreen(indexName, idleIndex);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
            indexDocs(idleIndex, randomIntBetween(1, 20));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        long maxUploadedFileAfterFirstIndex = translogReplicator.getMaxUploadedFile();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        assertDirectoryConsistency(blobs, translogBlobContainer, idleShardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(idleShardId);
            long maxReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .max()
                .getAsLong();
            assertThat(maxReferenced, lessThanOrEqualTo(maxUploadedFileAfterFirstIndex));
        }
    }

    public void testTranslogFileHoldDirectoryReflectsWhenFilesPruned() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder()
                .put(
                    StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(),
                    TimeValue.timeValueMillis(200)
                )
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        flush(indexName);

        assertBusy(() -> assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)));
        assertBusy(() -> assertTrue(translogBlobContainer.listBlobs(operationPurpose).isEmpty()));

        long minReferencedFile = translogReplicator.getMaxUploadedFile() + 1;
        indexDocs(indexName, randomIntBetween(1, 20));
        long maxUploadedFile = translogReplicator.getMaxUploadedFile();

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(translogReplicator.getMaxUploadedFile(), greaterThan(maxUploadedFile));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(shardId);
            long minReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .min()
                .getAsLong();
            assertThat(minReferenced, equalTo(minReferencedFile));
        }
    }

    public void testTranslogWillRecoveryAllFilesIfShardMissingDirectory() throws Exception {
        startMasterOnlyNode();
        startSearchNode();
        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        final String secondIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            secondIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build()
        );

        ensureGreen(indexName);

        final int docs = randomIntBetween(1, 10);
        indexDocs(indexName, docs);
        indexDocs(secondIndex, docs);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode), indexName);
        findIndexShard(resolveIndex(indexName), 0).failShard("broken", new Exception("boom"));

        indexDocs(secondIndex, docs);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", ""), indexName);

        ensureGreen(indexName);

        refresh(indexName);
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) docs));
        });
    }

    public void testTranslogReplicatorReaderEmitsMetrics() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1))
                .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
                .build()
        );
        ensureStableCluster(2);

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );

        var docsBatchOne = randomIntBetween(1, 10);
        var docsBatchTwo = randomIntBetween(1, 10);
        var docsBatchThree = randomIntBetween(1, 10);

        indexDocs(indexName, docsBatchOne);
        indexDocs(indexName, docsBatchTwo);

        // delete a random document to have `index` and `delete` operations be present in translog
        var response = indexDocs(indexName, docsBatchThree);
        var docIdToDelete = randomFrom(Arrays.stream(response.getItems()).map(BulkItemResponse::getId).toList());
        client().prepareDelete(indexName, docIdToDelete).get();

        ensureGreen(indexName);

        // resolve shard id
        var shardId = findIndexShard(indexName).shardId();

        var indexObjectStoreService = getObjectStoreService(indexNode);

        var recordingMeterRegistry = new RecordingMeterRegistry();
        try (
            var reader = new TranslogReplicatorReader(
                indexObjectStoreService.getTranslogBlobContainer(),
                shardId,
                0,
                Long.MAX_VALUE,
                0,
                () -> false,
                new TranslogRecoveryMetrics(recordingMeterRegistry)
            )
        ) {
            // read all translog operations
            Translog.Operation next = reader.next();
            while (next != null) {
                next = reader.next();
            }
        }

        BiFunction<InstrumentType, String, List<Measurement>> collectedMetrics = recordingMeterRegistry.getRecorder()::getMeasurements;

        var translogReadingTime = getSingleRecordedMetric(
            collectedMetrics,
            InstrumentType.LONG_HISTOGRAM,
            TranslogRecoveryMetrics.TRANSLOG_REPLAY_TIME_METRIC
        );
        assertThat(translogReadingTime.getLong(), greaterThan(0L));

        var translogFilesTotalCounters = getRecordedMetrics(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_FILES_TOTAL_METRIC,
            "translog_blob_type",
            "referenced"
        );
        assertThat(translogFilesTotalCounters.size(), equalTo(1));
        var translogFilesTotalCounter = translogFilesTotalCounters.get(0);
        // translog file is created based on time interval or size interval being exhausted, so it could be 1,2,3 or more
        // main concern of this test is fact of emitting metrics
        assertThat(translogFilesTotalCounter.getLong(), greaterThanOrEqualTo(1L));

        var translogFilesSizeCounters = getRecordedMetrics(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_FILES_SIZE_METRIC,
            "translog_blob_type",
            "referenced"
        );
        assertThat(translogFilesSizeCounters.size(), equalTo(1));
        var translogFilesSizeCounter = translogFilesTotalCounters.get(0);
        assertThat(translogFilesSizeCounter.getLong(), greaterThan(0L));

        var translogFilesNetworkTimeHistogram = getSingleRecordedMetric(
            collectedMetrics,
            InstrumentType.LONG_HISTOGRAM,
            TranslogRecoveryMetrics.TRANSLOG_FILES_NETWORK_TIME_METRIC
        );
        assertThat(translogFilesNetworkTimeHistogram.getLong(), greaterThan(0L));

        var translogIndexOperationsCounters = getRecordedMetrics(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_TOTAL_METRIC,
            "translog_op_type",
            "index"
        );
        assertThat(translogIndexOperationsCounters.size(), equalTo(1));
        var translogIndexOperationsCounter = translogIndexOperationsCounters.get(0);
        assertThat(translogIndexOperationsCounter.getLong(), equalTo((long) docsBatchOne + docsBatchTwo + docsBatchThree));

        var translogDeleteOperationsCounters = getRecordedMetrics(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_TOTAL_METRIC,
            "translog_op_type",
            "delete"
        );
        assertThat(translogDeleteOperationsCounters.size(), equalTo(1));
        var translogDeleteOperationsCounter = translogDeleteOperationsCounters.get(0);
        assertThat(translogDeleteOperationsCounter.getLong(), equalTo(1L));

        var translogNoopOperationsCounters = getRecordedMetrics(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_TOTAL_METRIC,
            "translog_op_type",
            "noop"
        );
        assertThat(translogNoopOperationsCounters.size(), equalTo(1));
        var translogNoopOperationsCounter = translogNoopOperationsCounters.get(0);
        assertThat(translogNoopOperationsCounter.getLong(), equalTo(0L));

        var translogOperationsSizeInBytes = getSingleRecordedMetric(
            collectedMetrics,
            InstrumentType.LONG_COUNTER,
            TranslogRecoveryMetrics.TRANSLOG_OPERATIONS_SIZE_METRIC
        );
        assertThat(translogOperationsSizeInBytes.getLong(), greaterThan(0L));
    }

    private static Measurement getSingleRecordedMetric(
        BiFunction<InstrumentType, String, List<Measurement>> metricGetter,
        InstrumentType type,
        String name
    ) {
        final List<Measurement> measurements = metricGetter.apply(type, name);
        assertFalse("Metric " + name + "is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }

    private static List<Measurement> getRecordedMetrics(
        BiFunction<InstrumentType, String, List<Measurement>> metricGetter,
        InstrumentType type,
        String name,
        String attributeName,
        String attributeValue
    ) {
        final List<Measurement> measurements = metricGetter.apply(type, name);
        assertFalse("Metric " + name + "is not recorded", measurements.isEmpty());
        return measurements.stream().filter(m -> m.attributes().get(attributeName).equals(attributeValue)).collect(Collectors.toList());
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogStressRecoveryTest() throws Exception {
        Settings isolatedNodeSettings = addIsolatedNodeSettings(Settings.builder()).build();
        runStressTest(
            4,
            isolatedNodeSettings,
            Failures.RESTART,
            Failures.REPLACE_FAILED_NODE,
            Failures.LOCAL_FAIL_SHARD,
            Failures.ISOLATED_INDEXING_NODE
        );
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogRestartOnlyStressRecoveryTest() throws Exception {
        // Restarts take the longest so lower failure count
        runStressTest(2, Failures.RESTART);
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogReplaceOnlyStressRecoveryTest() throws Exception {
        // Stop/Start take the longest so lower failure count
        runStressTest(2, Failures.REPLACE_FAILED_NODE);
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogLocalFailureOnlyStressRecoveryTest() throws Exception {
        runStressTest(3, Failures.LOCAL_FAIL_SHARD);
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogMasterFailureOnlyStressRecoveryTest() throws Exception {
        Settings heartbeatSettings = Settings.builder()
            .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
            .build();
        runStressTest(2, heartbeatSettings, Failures.MASTER_FAIL);
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogIsolatedNodeOnlyStressRecoveryTest() throws Exception {
        Settings isolatedNodeSettings = addIsolatedNodeSettings(Settings.builder()).build();
        runStressTest(2, isolatedNodeSettings, Failures.ISOLATED_INDEXING_NODE);
    }

    private static Settings.Builder addIsolatedNodeSettings(Settings.Builder builder) {
        return builder.put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "500ms")
            .put(PUBLISH_TIMEOUT_SETTING.getKey(), "1s");
    }

    private void runStressTest(int failureCount, Failures... failureTypes) throws Exception {
        runStressTest(failureCount, Settings.EMPTY, failureTypes);
    }

    private void runStressTest(int failureCount, Settings additionalSettings, Failures... failureTypes) throws Exception {
        TimeValue flushInterval = TimeValue.timeValueMillis(rarely() ? 200 : randomLongBetween(25, 100));
        logger.info("running test with translog flush interval {}", flushInterval);
        Settings settings = Settings.builder().put(additionalSettings).put(FLUSH_INTERVAL_SETTING.getKey(), flushInterval).build();

        startIndexingAndMasterNode(settings);
        startIndexingAndMasterNode(settings);
        startIndexingAndMasterNode(settings);
        startSearchNode(settings);
        ensureStableCluster(4);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(4, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        AtomicLong minExpectedDocs = new AtomicLong();

        ArrayList<CountDownLatch> failureLatches = new ArrayList<>(failureCount);
        int reqN = 0;
        for (int i = 0; i < failureCount; i++) {
            int failThreshold = randomIntBetween(30, 60);
            failureLatches.add(new CountDownLatch(reqN + failThreshold));
            reqN += failThreshold;
        }
        reqN += randomIntBetween(30, 60);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CountDownLatch allReqLatch = new CountDownLatch(reqN);
        Set<String> successes = ConcurrentCollections.newConcurrentSet();
        Set<String> failures = ConcurrentCollections.newConcurrentSet();

        logger.info("running test with {} requests", reqN);

        for (int n = 0; n < reqN; ++n) {
            executorService.execute(() -> {
                TimeValue bulkRequestTimeout = BulkShardRequest.DEFAULT_TIMEOUT;
                try {
                    var bulkRequest = client().prepareBulk();
                    // Occasionally set timeout to avoid retries
                    if (randomBoolean() && randomBoolean()) {
                        TimeValue timeout = TimeValue.timeValueMillis(100);
                        bulkRequest.setTimeout(timeout);
                        bulkRequestTimeout = timeout;
                    }
                    int numDocs = randomIntBetween(10, 50);
                    for (int i = 0; i < numDocs; i++) {
                        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                    }
                    var bulkResponse = bulkRequest.get();

                    Arrays.stream(bulkResponse.getItems()).forEach(bulkItemResponse -> {
                        if (bulkItemResponse.isFailed()) {
                            failures.add(bulkItemResponse.getId());
                        } else {
                            minExpectedDocs.incrementAndGet();
                            successes.add(bulkItemResponse.getId());
                        }
                    });

                    // Flush approximately every 32 requests
                    if (randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean()) {
                        // Do not use the test flush() helper method as it can trigger an assertion if a flush fails due to a node restart.
                        indicesAdmin().prepareFlush(indexName).get();
                    }
                } catch (Exception e) {
                    logger.warn(Strings.format("exception on indexing thread [bulk request timeout=%s]", bulkRequestTimeout), e);
                } finally {
                    failureLatches.forEach(CountDownLatch::countDown);
                    allReqLatch.countDown();
                }
            });
        }

        try {
            for (CountDownLatch latch : failureLatches) {
                safeAwait(latch);
                induceFailures(settings, indexName, failureTypes);
            }

            safeAwait(allReqLatch);

            refresh(indexName);

            SearchResponse response = prepareSearch(indexName).setQuery(QueryBuilders.idsQuery().addIds(failures.toArray(new String[0])))
                .get();
            long failureHits = response.getHits().getTotalHits().value;
            response.decRef();
            logger.info(
                "Found ["
                    + failureHits
                    + "] hits matching failure ids. This is not an error condition as docs can be accepted by the system and still be "
                    + "indicated as failed to the client."
            );

            assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(minExpectedDocs.get()));
            });

            assertResponse(
                prepareSearch(indexName).setQuery(QueryBuilders.idsQuery().addIds(successes.toArray(new String[0]))),
                searchResponse -> {
                    assertNoFailures(searchResponse);
                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) successes.size()));
                }
            );
        } finally {
            long outstandingRequests = allReqLatch.getCount();
            if (outstandingRequests > 0) {
                logger.warn("test finished with {} outstanding requests", outstandingRequests);
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private enum Failures {
        RESTART,
        REPLACE_FAILED_NODE,
        LOCAL_FAIL_SHARD,
        REMOTE_FAIL_SHARD,
        MASTER_FAIL,
        ISOLATED_INDEXING_NODE
    }

    private void induceFailures(Settings settings, String indexName, Failures... failureTypes) throws Exception {
        Failures failure = randomFrom(failureTypes);
        logger.info("inducing failure of type: {}", failure.name());
        switch (failure) {
            case RESTART -> {
                String nodeToRestart = nonMasterIndexingNode();
                internalCluster().restartNode(nodeToRestart);
                ensureStableCluster(4);
            }
            case REPLACE_FAILED_NODE -> {
                String nodeToRestart = nonMasterIndexingNode();
                internalCluster().stopNode(nodeToRestart);
                startIndexingAndMasterNode(settings);
                ensureStableCluster(4);
            }
            case LOCAL_FAIL_SHARD -> {
                IndexShard indexShard = findIndexShard(resolveIndex(indexName), randomFrom(0, 1, 2, 3));
                indexShard.failShard("broken", new Exception("boom local"));
            }
            case REMOTE_FAIL_SHARD -> {
                IndexShard indexShard = findIndexShard(resolveIndex(indexName), randomFrom(0, 1, 2, 3));
                ShardStateAction shardStateAction = internalCluster().getInstance(
                    ShardStateAction.class,
                    internalCluster().getRandomNodeName()
                );
                ShardRouting shardRouting = indexShard.routingEntry();
                PlainActionFuture<Void> listener = new PlainActionFuture<>();
                shardStateAction.remoteShardFailed(
                    indexShard.shardId(),
                    shardRouting.allocationId().getId(),
                    indexShard.getOperationPrimaryTerm(),
                    true,
                    "broken",
                    new Exception("boom remote"),
                    listener
                );
                listener.actionGet();
            }
            case MASTER_FAIL -> {
                internalCluster().stopCurrentMasterNode();
                startIndexingAndMasterNode(settings);
                ensureStableCluster(4);
            }
            case ISOLATED_INDEXING_NODE -> {
                String isolatedNode = nonMasterIndexingNode();
                final MockTransportService nodeATransportService = MockTransportService.getInstance(isolatedNode);
                final MockTransportService masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
                try {
                    PlainActionFuture<Void> removedNode = new PlainActionFuture<>();

                    final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
                    masterClusterService.addListener(clusterChangedEvent -> {
                        if (removedNode.isDone() == false
                            && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(isolatedNode))) {
                            removedNode.onResponse(null);
                        }
                    });
                    masterTransportService.addUnresponsiveRule(nodeATransportService);
                    removedNode.actionGet();
                    ClusterHealthRequest healthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(30))
                        .waitForStatus(ClusterHealthStatus.YELLOW)
                        .waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)
                        .waitForNoInitializingShards(true)
                        .waitForNodes(Integer.toString(3));

                    internalCluster().masterClient().admin().cluster().health(healthRequest).actionGet();
                } finally {
                    masterTransportService.clearAllRules();
                }
            }
        }
        ensureGreen(indexName);
    }

    private static String nonMasterIndexingNode() {
        String masterName = internalCluster().getMasterName();
        return Stream.generate(() -> internalCluster().getNodeNameThat(settings -> {
            List<DiscoveryNodeRole> discoveryNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings);
            return discoveryNodeRoles.contains(DiscoveryNodeRole.INDEX_ROLE);
        })).filter(n -> masterName.equals(n) == false).findFirst().get();
    }

    private void startIndexingAndMasterNode(Settings additionalSettings) {
        startMasterAndIndexNode(
            Settings.builder()
                .put(additionalSettings)
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
    }

    private static void assertDirectoryConsistency(List<BlobMetadata> blobs, BlobContainer translogBlobContainer, ShardId shardId)
        throws IOException {
        long totalOps = 0;
        HashSet<Long> referencedFiles = new HashSet<>();
        for (BlobMetadata blob : blobs) {
            long generation = Long.parseLong(blob.name());
            try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, blob.name()))) {
                CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(blob.name(), streamInput);
                TranslogMetadata metadata = header.metadata().get(shardId);
                totalOps += metadata.totalOps();
                assertThat(metadata.directory().estimatedOperationsToRecover(), equalTo(totalOps));
                Set<Long> actualReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                    .mapToLong(r -> generation - r)
                    .boxed()
                    .collect(Collectors.toSet());

                assertThat(actualReferenced, equalTo(referencedFiles));
                if (metadata.totalOps() > 0) {
                    referencedFiles.add(generation);
                }
            }
        }
    }

    private static void assertTranslogBlobsExist(Set<TranslogReplicator.BlobTranslogFile> shouldExist, BlobContainer container)
        throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(container.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

}
