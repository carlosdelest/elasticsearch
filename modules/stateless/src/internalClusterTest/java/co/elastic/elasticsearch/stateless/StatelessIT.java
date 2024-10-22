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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.index.IndexSettings.STATELESS_DEFAULT_REFRESH_INTERVAL;
import static org.elasticsearch.indices.IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

// Disabling WindowsFS because it prevents file deletions and ExtrasFS because it adds unnecessary files in Lucene index and tests in this
// class verify the content of Lucene directories
@LuceneTestCase.SuppressFileSystems(value = { "WindowsFS", "ExtrasFS" })
public class StatelessIT extends AbstractStatelessIntegTestCase {

    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(BlobStoreHealthIndicator.POLL_INTERVAL, "1s")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(MockRepository.Plugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    public void testCompoundCommitHasNodeEphemeralId() throws Exception {
        startMasterOnlyNode();

        String indexNodeName = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        indexDocumentsWithFlush(indexName);

        assertObjectStoreConsistentWithIndexShards();

        Index index = resolveIndex(indexName);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        ObjectStoreService objectStoreService = getObjectStoreService(indexNodeName);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, indexNodeName);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        final long directoryGeneration = Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        assertBusy(() -> {
            final BatchedCompoundCommit latestUploadedBcc = readLatestUploadedBccUptoGen(blobContainerForCommit, directoryGeneration);
            StatelessCompoundCommit commit = latestUploadedBcc.lastCompoundCommit();
            assertThat(commit.generation(), equalTo(directoryGeneration));
            assertThat(
                "Expected that the compound commit has the ephemeral Id of the indexing node",
                commit.nodeEphemeralId(),
                equalTo(clusterService.localNode().getEphemeralId())
            );
        });
    }

    public void testClusterCanFormWithStatelessEnabled() {
        startMasterOnlyNode();

        final int numIndexNodes = randomIntBetween(1, 5);
        startIndexNodes(numIndexNodes);
        ensureStableCluster(numIndexNodes + 1);

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(Stateless.class))
            .toList();
        assertThat(plugins.size(), greaterThan(0));
    }

    public void testRefreshIntervalSetting() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());

        final String fastIndexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());

        final Map<Index, Integer> indices = resolveIndices();
        Index index = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        Index fastIndex = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(fastIndexName)).findAny().get().getKey();

        // Non fast refresh index

        assertNull("setting should not be set", getRefreshIntervalSetting(indexName, false));
        assertEquals(
            "unexpected default value for non fast refresh indices",
            STATELESS_DEFAULT_REFRESH_INTERVAL,
            getRefreshIntervalSetting(indexName, true)
        );
        assertRefreshIntervalConcreteValue(index, STATELESS_DEFAULT_REFRESH_INTERVAL);
        assertTrue(setRefreshIntervalSetting(indexName, TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueSeconds(120), getRefreshIntervalSetting(indexName, false));
        assertRefreshIntervalConcreteValue(index, TimeValue.timeValueSeconds(120));
        // TODO (ES-6244): try setting to less than 5sec and assert there is a validation exception.
        assertTrue(setRefreshIntervalSetting(indexName, TimeValue.MINUS_ONE));
        assertEquals(TimeValue.MINUS_ONE, getRefreshIntervalSetting(indexName, false));
        assertRefreshIntervalConcreteValue(index, TimeValue.MINUS_ONE);
        assertTrue(setRefreshIntervalSetting(indexName, null));
        assertEquals(STATELESS_DEFAULT_REFRESH_INTERVAL, getRefreshIntervalSetting(indexName, true));
        assertRefreshIntervalConcreteValue(index, STATELESS_DEFAULT_REFRESH_INTERVAL);

        // Fast refresh index. The refresh interval setting should behave similarly to stateful.

        assertNull("setting should not be set", getRefreshIntervalSetting(fastIndexName, false));
        assertEquals(
            "unexpected default value for non fast refresh indices",
            TimeValue.timeValueSeconds(1),
            getRefreshIntervalSetting(fastIndexName, true)
        );
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(1));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueSeconds(120), getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(120));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.timeValueMillis(100)));
        assertEquals(TimeValue.timeValueMillis(100), getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueMillis(100));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.MINUS_ONE));
        assertEquals(TimeValue.MINUS_ONE, getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.MINUS_ONE);
        assertTrue(setRefreshIntervalSetting(fastIndexName, null));
        assertEquals(TimeValue.timeValueSeconds(1), getRefreshIntervalSetting(fastIndexName, true));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(1));
    }

    public void testScheduledRefreshBypassesSearchIdleness() throws Exception {
        startMasterOnlyNode();
        startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Settings.Builder indexSettings = indexSettings(1, 0).put(
            IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(),
            TimeValue.timeValueMillis(1)
        );
        if (rarely() == false) {
            indexSettings.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), IndexSettings.STATELESS_MIN_NON_FAST_REFRESH_INTERVAL);
        }
        createIndex(indexName, indexSettings.build());

        indexDocs(indexName, randomIntBetween(1, 5));

        IndexShard indexShard = findIndexShard(indexName);
        long genBefore = indexShard.commitStats().getGeneration();

        assertBusy(
            () -> assertThat(
                "expected a scheduled refresh to cause the non fast refresh shard to flush and produce a new commit generation",
                indexShard.commitStats().getGeneration(),
                greaterThan(genBefore)
            ),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testUploadToObjectStore() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numberOfShards, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        indexDocumentsWithFlush(indexName);
    }

    public void testTranslogIsSyncedToObjectStoreDuringIndexing() throws Exception {
        startMasterOnlyNode();
        startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final Map<Index, Integer> indices = resolveIndices();
        Optional<Map.Entry<Index, Integer>> index = indices.entrySet()
            .stream()
            .filter(e -> indexName.equals(e.getKey().getName()))
            .findFirst();
        assertTrue(index.isPresent());

        Map.Entry<Index, Integer> entry = index.get();
        DiscoveryNode indexNode = findIndexNode(entry.getKey(), 0);
        final ShardId shardId = new ShardId(entry.getKey(), 0);

        indexDocs(indexName, 1);

        // Check that the translog on the object store contains the correct sequence numbers and number of operations
        var indexObjectStoreService = getObjectStoreService(indexNode.getName());
        var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), shardId);
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long totalOps = 0;
        Translog.Operation next = reader.next();
        while (next != null) {
            maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
            totalOps++;
            next = reader.next();
        }
        assertThat(maxSeqNo, equalTo(0L));
        assertThat(totalOps, equalTo(1L));
    }

    public void testGlobalCheckpointOnlyAdvancesAfterObjectStoreSync() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = 1;
        String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numberOfShards, 0).build());
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final Map<Index, Integer> indices = resolveIndices();
        Optional<Map.Entry<Index, Integer>> index = indices.entrySet()
            .stream()
            .filter(e -> indexName.equals(e.getKey().getName()))
            .findFirst();
        assertTrue(index.isPresent());

        // Ensure that an automatic flush cannot clean translog
        ObjectStoreService objectStoreService = getObjectStoreService(indexNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
        repository.setRandomIOExceptionPattern(".*translog.*");

        var bulkRequest = client().prepareBulk();
        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        ActionFuture<BulkResponse> bulkFuture = bulkRequest.execute();

        IndexShard indexShard = findIndexShard(index.get().getKey(), 0);

        assertBusy(() -> {
            assertThat(indexShard.getEngineOrNull().getProcessedLocalCheckpoint(), greaterThanOrEqualTo(0L));
            assertThat(indexShard.getEngineOrNull().getTranslogLastWriteLocation().translogLocation(), greaterThanOrEqualTo(0L));
        });
        // Sleep to allow local file system translog sync to complete which historically would have advanced local checkpoint. But now it
        // should no longer advance local checkpoint
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

        // Submit GlobalCheckpointSyncAction. This is necessary to propagate the local checkpoint advancement which the bulk request does
        // not do because it is currently blocked.
        client().execute(GlobalCheckpointSyncAction.TYPE, new GlobalCheckpointSyncAction.Request(indexShard.shardId())).actionGet();

        PlainActionFuture<Long> globalCheckpointFuture1 = new PlainActionFuture<>();
        indexShard.addGlobalCheckpointListener(0, new GlobalCheckpointListeners.GlobalCheckpointListener() {
            @Override
            public Executor executor() {
                return indexShard.getThreadPool().generic();
            }

            @Override
            public void accept(long globalCheckpoint, Exception e) {
                if (e != null) {
                    globalCheckpointFuture1.onFailure(e);
                } else {
                    globalCheckpointFuture1.onResponse(globalCheckpoint);
                }

            }
        }, TimeValue.timeValueMillis(100L));

        try {
            UncategorizedExecutionException uee = expectThrows(UncategorizedExecutionException.class, globalCheckpointFuture1::actionGet);
            assertThat(uee.getCause().getCause(), instanceOf(TimeoutException.class));
        } finally {
            repository.setRandomControlIOExceptionRate(0.0);
            repository.setRandomDataFileIOExceptionRate(0.0);
        }

        assertBusy(() -> assertThat(indexShard.getLocalCheckpoint(), greaterThanOrEqualTo(0L)));

        bulkFuture.actionGet();
    }

    public void testAllTranslogOperationsAreWrittenToObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        List<String> indexingNodes = startIndexNodes(numberOfShards);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(numberOfShards, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        // Ensure that an automatic flush cannot clean translog
        for (String indexingNode : indexingNodes) {
            ObjectStoreService objectStoreService = getObjectStoreService(indexingNode);
            MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
            repository.setRandomControlIOExceptionRate(1.0);
            repository.setRandomDataFileIOExceptionRate(1.0);
            repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
            repository.setRandomIOExceptionPattern(".*stateless_commit_.*");
        }

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        assertReplicatedTranslogConsistentWithShards();

        // Allow flushes again
        for (String indexingNode : indexingNodes) {
            ObjectStoreService objectStoreService = getObjectStoreService(indexingNode);
            MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
            repository.setRandomControlIOExceptionRate(0.0);
            repository.setRandomDataFileIOExceptionRate(0.0);
        }
    }

    public void testDownloadNewCommitsFromObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 2);
        startIndexNodes(numberOfShards, disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // Disable scheduled refreshes so it doesn't add non-uploaded commits to the BCC
        createIndex(indexName, indexSettings(numberOfShards, 1).put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        // Index more documents
        indexDocumentsWithFlush(indexName);

        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }

        // Index more documents
        indexDocumentsWithFlush(indexName);

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }
    }

    public void testDownloadNewReplicasFromObjectStore() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(indexName, indexSettings(numberOfShards, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);
        for (int i = 0; i < 3; i++) {
            indexDocumentsWithFlush(indexName);
            assertObjectStoreConsistentWithIndexShards();
        }

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        assertObjectStoreConsistentWithSearchShards();
    }

    public void testCreatesSearchShardsOfClosedIndex() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(indexName, indexSettings(numberOfShards, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);
        indexDocumentsWithFlush(indexName);
        assertObjectStoreConsistentWithIndexShards();

        assertAcked(client().admin().indices().prepareClose(indexName));

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        // TODO assertObjectStoreConsistentWithSearchShards(); doesn't work yet because closing the index incremented the primary term
    }

    public void testSetsRecyclableBigArraysInTranslogReplicator() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertBusy(() -> {
            var bigArrays = getTranslogReplicator(masterAndIndexNode).bigArrays();
            assertNotNull(bigArrays);
            assertNotNull(bigArrays.breakerService());
        });
    }

    public void testIndicesSegments() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 3);
        startIndexNodes(numberOfShards);
        final int numberOfReplicas = randomIntBetween(0, 3);
        startSearchNodes(numberOfReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numberOfShards, numberOfReplicas).build());
        ensureGreen(indexName);

        final int iters = randomIntBetween(0, 5);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        if (iters > 0 || randomBoolean()) {
            refresh(indexName);
        }

        var indicesSegments = client().admin().indices().prepareSegments(indexName).get();
        assertThat(indicesSegments.getSuccessfulShards(), equalTo(numberOfShards + numberOfShards * numberOfReplicas));
        assertThat(indicesSegments.getShardFailures().length, equalTo(0));
        assertThat(indicesSegments.getIndices().size(), equalTo(1));

        var indices = indicesSegments.getIndices().get(indexName);
        assertThat(indices, notNullValue());
        assertThat(indices.getShards().size(), equalTo(numberOfShards));

        var index = resolveIndex(indexName);
        for (int shard = 0; shard < numberOfShards; shard++) {
            var shardSegments = indices.getShards().get(shard);
            assertThat(shardSegments, notNullValue());
            assertThat(shardSegments.shards(), notNullValue());
            assertThat(shardSegments.shards().length, equalTo(1 + numberOfReplicas));
            var shardId = new ShardId(index, shard);
            assertThat(shardSegments.shardId(), equalTo(shardId));

            var indexShard = Arrays.stream(shardSegments.shards())
                .filter(segments -> segments.getShardRouting().isPromotableToPrimary())
                .findAny()
                .orElseThrow(() -> new AssertionError("no index shard found for " + shardId));
            assertThat(indexShard, notNullValue());

            var searchShards = Arrays.stream(shardSegments.shards()).filter(segments -> segments.getShardRouting().isSearchable()).toList();
            assertThat(searchShards.size(), equalTo(numberOfReplicas));

            for (var searchShard : searchShards) {
                assertThat(searchShard.getNumberOfSearch(), equalTo(indexShard.getNumberOfSearch()));
                assertThat(searchShard.getNumberOfCommitted(), equalTo(indexShard.getNumberOfCommitted()));
                assertThat(searchShard.getSegments().size(), equalTo(indexShard.getSegments().size()));

                for (var indexShardSegment : indexShard.getSegments()) {
                    var searchShardSegment = searchShard.getSegments()
                        .stream()
                        .filter(segment -> segment.getName().equals(indexShardSegment.getName()))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("search shard has no corresponding segment " + indexShardSegment.getName()));
                    assertThat(searchShardSegment.getGeneration(), equalTo(indexShardSegment.getGeneration()));
                    assertThat(searchShardSegment.isCommitted(), equalTo(indexShardSegment.isCommitted()));
                    assertThat(searchShardSegment.getNumDocs(), equalTo(indexShardSegment.getNumDocs()));
                    assertThat(searchShardSegment.getDeletedDocs(), equalTo(indexShardSegment.getDeletedDocs()));
                    assertThat(searchShardSegment.getSize(), equalTo(indexShardSegment.getSize()));
                    assertThat(searchShardSegment.getVersion(), equalTo(indexShardSegment.getVersion()));
                    assertThat(searchShardSegment.getMergeId(), equalTo(indexShardSegment.getMergeId()));
                    assertThat(searchShardSegment.getAttributes(), equalTo(indexShardSegment.getAttributes()));
                    assertThat(searchShardSegment.getSegmentSort(), equalTo(indexShardSegment.getSegmentSort()));
                }
            }
        }
    }

    public void testUploadToObjectStoreAfterShardIsClosed() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        if (randomBoolean()) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        assertObjectStoreConsistentWithIndexShards();

        indexDocs(indexName, randomIntBetween(1, 100));

        // block the object store uploading thread pool with tasks before triggering a flush and closing the shard
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        final String uploadThreadPoolName = ThreadPool.Names.SNAPSHOT;
        final int maxUploadTasks = threadPool.info(uploadThreadPoolName).getMax();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch taskStartedLatch = new CountDownLatch(maxUploadTasks);
        for (int i = 0; i < maxUploadTasks; i++) {
            threadPool.executor(uploadThreadPoolName).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    taskStartedLatch.countDown();
                    latch.await();
                }
            });
        }
        try {
            taskStartedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        int active = -1;
        for (var stats : threadPool.stats()) {
            if (stats.name().equals(uploadThreadPoolName)) {
                active = stats.active();
                break;
            }
        }
        assertThat(active, equalTo(maxUploadTasks));

        ActionFuture<BroadcastResponse> flushFuture = client().admin().indices().prepareFlush(indexName).execute();

        var indexShard = findIndexShard(resolveIndex(indexName), 0);
        // we must hold a ref on the store to allow assertThatObjectStoreIsConsistentWithLastCommit
        indexShard.store().incRef();
        try {
            var future = client().admin().indices().close(new CloseIndexRequest(indexName).waitForActiveShards(ActiveShardCount.NONE));
            latch.countDown();
            assertThatObjectStoreIsConsistentWithLastCommit(indexShard);
            assertAcked(future.actionGet());
            assertEquals(0, flushFuture.actionGet().getFailedShards());
        } finally {
            indexShard.store().decRef();
        }
    }

    public void testIndexSearchDirectoryPruned() throws Exception {
        startMasterOnlyNode();
        startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);
        IndexShard shard = findIndexShard(indexName);
        Directory directory = shard.store().directory();
        BlobStoreCacheDirectory blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(directory);
        // nothing deleted yet so we expect same contents.
        String[] originalFiles = blobStoreCacheDirectory.listAll();
        assertThat(originalFiles, equalTo(directory.listAll()));
        if (randomBoolean()) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        assertObjectStoreConsistentWithIndexShards();

        Set<String> secondFileSet = Sets.union(toSet(directory.listAll()), toSet(originalFiles));
        assertBusy(() -> assertThat(toSet(blobStoreCacheDirectory.listAll()), equalTo(secondFileSet)));
        indexDocs(indexName, randomIntBetween(1, 100));
        flush(indexName);

        // retained by external reader manager.
        assertThat(toSet(blobStoreCacheDirectory.listAll()), hasItems(secondFileSet.toArray(String[]::new)));

        refresh(indexName);

        // only updated on commit so provoke one.
        indexDocs(indexName, randomIntBetween(1, 100));
        flush(indexName);

        // expect at least one deleted file, the segments_N file.
        assertBusy(() -> assertThat(toSet(blobStoreCacheDirectory.listAll()), not(hasItems(secondFileSet.toArray(String[]::new)))));

        forceMerge();
        refresh(indexName);
        indexDocs(indexName, randomIntBetween(1, 100));
        flush(indexName);

        // all from secondFileSet are gone.
        assertBusy(() -> assertThat(Sets.intersection(toSet(blobStoreCacheDirectory.listAll()), secondFileSet), empty()));

    }

    private static Set<String> toSet(String[] strings) {
        return Set.of(strings);
    }

    public void testBlobStoreHealthIndicator() throws Exception {
        startMasterOnlyNode();
        HealthService healthService = internalCluster().getInstance(HealthService.class);
        assertBusy(() -> {
            PlainActionFuture<List<HealthIndicatorResult>> response = new PlainActionFuture<>();
            healthService.getHealth(client(), "blob_store", true, randomIntBetween(1, 10), response);
            List<HealthIndicatorResult> healthIndicatorResults = response.actionGet();
            assertThat(healthIndicatorResults.size(), is(1));
            HealthIndicatorResult result = healthIndicatorResults.get(0);
            assertThat(result.status(), is(HealthStatus.GREEN));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            result.details().toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput());
            Map<String, Object> details = parser.map();
            // This will ensure that the check has been performed
            assertThat(details.containsKey("time_since_last_update_millis"), is(true));
        });
    }

    public void testAutoExpandReplicasSettingsAreIgnored() throws Exception {
        startMasterOnlyNode();
        var indexNodes = startIndexNodes(2);
        var searchNodes = startSearchNodes(randomFrom(1, 2));
        ensureStableCluster(3 + searchNodes.size());

        final String indexName = randomIdentifier();
        var autoExpandConfiguration = randomFrom("0-all", "0-20", "0-3", "0-1");
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, autoExpandConfiguration);

        createIndex(indexName, indexSettings.build());

        if (randomBoolean()) {
            var shutdownNode = randomFrom(Stream.concat(indexNodes.stream(), searchNodes.stream()).toList());
            var shutdownNodeId = client().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .nodes()
                .resolveNode(shutdownNode)
                .getId();
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    shutdownNodeId,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "Shutdown for tests",
                    null,
                    null,
                    TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                )
            ).get();
            var shutdownStatus = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT)
            ).actionGet(10, TimeUnit.SECONDS);
            assertThat(shutdownStatus.getShutdownStatuses(), hasSize(1));
            if (searchNodes.equals(List.of(shutdownNode))) {
                assertThat(
                    shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(),
                    equalTo(SingleNodeShutdownMetadata.Status.STALLED)
                );
            } else {
                assertThat(
                    shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(),
                    oneOf(SingleNodeShutdownMetadata.Status.COMPLETE, SingleNodeShutdownMetadata.Status.IN_PROGRESS)
                );
            }
        }

        assertThat(
            indicesAdmin().prepareGetSettings(indexName)
                .setNames("index.number_of_replicas")
                .get()
                .getSetting(indexName, "index.number_of_replicas"),
            is(equalTo("1"))
        );
        ensureGreen(indexName);
        assertEquals(clusterService().state().routingTable().index(indexName).shard(0).replicaShards().size(), 1);
    }

    public void testBackgroundMergeCommitAfterRelocationHasStartedDoesNotSendANewCommitNotification() throws Exception {
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 12)
            // Ensure that merges are flushed immediately
            .put(SHARD_INACTIVE_TIME_SETTING.getKey(), TimeValue.ZERO)
            // Disable background flushes
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        var indexNode1 = startMasterAndIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        ensureStableCluster(2);
        var indexNode1EphemeralId = client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .getNodes()
            .resolveNode(indexNode1)
            .getEphemeralId();

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var indexNode2 = startMasterAndIndexNode(nodeSettings);

        for (int i = 0; i < 5; i++) {
            indexDocs(indexName, 300);
            refresh(indexName);
        }

        var indexShard = findIndexShard(indexName);
        var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        var statelessCommitService = indexEngine.getStatelessCommitService();
        var latestGenerationBeforeRelocation = indexEngine.getLastCommittedSegmentInfos().getGeneration();
        var mergeCommitGeneration = latestGenerationBeforeRelocation + 1;

        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                NewCommitNotificationRequest newCommitNotificationRequest = (NewCommitNotificationRequest) request;
                if (newCommitNotificationRequest.getCompoundCommit().nodeEphemeralId().equals(indexNode1EphemeralId)
                    && newCommitNotificationRequest.getGeneration() > latestGenerationBeforeRelocation) {
                    fail("Unexpected new commit notification for merge commit " + newCommitNotificationRequest);
                }
                handler.messageReceived(request, channel, task);
            });

        var primaryContextHandoffSent = new CountDownLatch(1);
        AtomicReference<CheckedRunnable<Exception>> runPrimaryContextHandOff = new AtomicReference<>();
        MockTransportService.getInstance(indexNode2)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                runPrimaryContextHandOff.set(() -> handler.messageReceived(request, channel, task));
                primaryContextHandoffSent.countDown();
            });

        logger.info("--> relocating shard 0 from {} to {}", indexNode1, indexNode2);
        var relocationFuture = client().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(
                new MoveAllocationCommand(indexName, 0, indexNode1, indexNode2)
            )
        );

        safeAwait(primaryContextHandoffSent);

        var mergeCommitBCCUploadListener = new PlainActionFuture<Void>();
        statelessCommitService.addListenerForUploadedGeneration(
            indexEngine.config().getShardId(),
            mergeCommitGeneration,
            mergeCommitBCCUploadListener
        );

        var forceMergeThread = new Thread(() -> {
            try {
                indexShard.forceMerge(new ForceMergeRequest().maxNumSegments(1));
            } catch (UnavailableShardsException | AlreadyClosedException e) {
                // Force merge checks if the engine is still open at the end, and sometimes it might
                // throw an AlreadyClosedException even after the commit is already processed by ShardCommitState
            } catch (IOException e) {
                fail(e);
            }
        }, "force-merge-thread");
        forceMergeThread.start();

        runPrimaryContextHandOff.get().run();
        forceMergeThread.join();
        safeGet(relocationFuture);
        ensureGreen(indexName);

        // TODO run a concurrent search to exercise more code paths that could lead to transient corruption issues

        indexDocs(indexName, 200);
        refresh(indexName);

        // The upload task for the merge commit retries a few times, and it holds a reference to the merge commit, if we don't wait
        // until that task finishes the test might fail since that task holds a Lucene commit reference after the test has finished
        var exception = expectThrows(ExecutionException.class, mergeCommitBCCUploadListener::get);
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause(), instanceOf(UnavailableShardsException.class));
    }

    protected static TimeValue getRefreshIntervalSetting(String index, boolean includeDefaults) throws Exception {
        var request = new GetSettingsRequest();
        request = request.indices(index).includeDefaults(includeDefaults);
        GetSettingsResponse response = client().admin().indices().getSettings(request).get();
        String value = response.getSetting(index, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
        return TimeValue.parseTimeValue(value, null, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
    };

    protected static void assertRefreshIntervalConcreteValue(Index index, TimeValue refreshInterval) throws Exception {
        boolean found = false;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                found = true;
                assertThat(
                    indexService + " did not match refresh interval",
                    indexService.getIndexSettings().getRefreshInterval(),
                    equalTo(refreshInterval)
                );
            }
        }
        assertThat(found, equalTo(true));
    };

    protected static boolean setRefreshIntervalSetting(String index, TimeValue timeValue) throws Exception {
        var response = client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(index).settings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), timeValue == null ? null : timeValue.getStringRep())
                )
            )
            .get();
        return response.isAcknowledged();
    };

    public void testSegmentsFilesDeletedAfterUpload() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(indexName);

        var shard = findIndexShard(resolveIndex(indexName), 0);
        assertNotNull(shard);
        var engine = shard.getEngineOrNull();
        assertThat(engine, instanceOf(IndexEngine.class));
        var statelessCommitService = ((IndexEngine) engine).getStatelessCommitService();
        assertNotNull(statelessCommitService);

        final Set<String> uploadedFiles = ConcurrentCollections.newConcurrentSet();
        statelessCommitService.addConsumerForNewUploadedBcc(
            shard.shardId(),
            info -> uploadedFiles.addAll(
                info.uploadedBcc().compoundCommits().stream().flatMap(cc -> cc.commitFiles().keySet().stream()).toList()
            )
        );

        var numDocs = 100;
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("foo", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        assertNoFailures(bulkRequest.get());
        assertNoFailures(indicesAdmin().prepareFlush(indexName).setForce(true).get());

        assertBusy(() -> assertThat(uploadedFiles.isEmpty(), is(false)));
        assertSegmentsFilesDeletedAfterUpload(shard);

        var numUpdates = randomIntBetween(1, 100);
        bulkRequest = client().prepareBulk();
        for (int i = 0; i < numUpdates; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).id(String.valueOf(randomIntBetween(0, numDocs - 1)))
                    .source("foo", randomUnicodeOfCodepointLengthBetween(1, 25), "bar", randomUnicodeOfCodepointLengthBetween(1, 25))
            );
        }
        assertNoFailures(bulkRequest.get());
        flush(indexName);

        assertSegmentsFilesDeletedAfterUpload(shard);
    }

    public void testCanRestartMasterNodes() throws Exception {
        var masterNode = internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
        internalCluster().restartNode(masterNode);
    }

    private static void assertSegmentsFilesDeletedAfterUpload(IndexShard indexShard) throws Exception {
        assertBusy(() -> {
            try (var dir = Files.list(indexShard.shardPath().resolveIndex())) {
                var files = dir.map(path -> path.getFileName().toString())
                    .filter(fileName -> fileName.equals("write.lock") == false)
                    .collect(Collectors.toSet());
                assertThat("Lucene files should have been deleted from shard but got: " + files, files.isEmpty(), is(true));
            }
        });
    }

    protected static void indexDocumentsWithFlush(String indexName) {
        indexDocumentsThenFlushOrRefreshOrForceMerge(indexName, StatelessIT::assertObjectStoreConsistentWithIndexShards, () -> {
            client().admin().indices().prepareFlush(indexName).setForce(false).setWaitIfOngoing(false).get();
            assertObjectStoreConsistentWithIndexShards();
        }, StatelessIT::assertObjectStoreConsistentWithIndexShards);
    }

    private static void assertObjectStoreConsistentWithIndexShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    private static void assertObjectStoreConsistentWithSearchShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    private static void assertObjectStoreConsistentWithShards(DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findShard(entry.getKey(), shardId, nodeRole, shardRole));
            }
        }
    }

    private static void assertThatObjectStoreIsConsistentWithLastCommit(final IndexShard indexShard) {
        final Store store = indexShard.store();
        store.incRef();
        try {
            ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            // can take some time for files to be uploaded to the object store
            assertBusy(() -> {
                // New commit can be uploaded after we read segmentInfos, so we bound the read by the known generation
                final var latestUploadedBcc = readLatestUploadedBccUptoGen(blobContainerForCommit, segmentInfos.getGeneration());
                StatelessCompoundCommit commit = latestUploadedBcc.lastCompoundCommit();

                assertThat(commit.primaryTermAndGeneration().generation(), equalTo(segmentInfos.getGeneration()));
                var localFiles = segmentInfos.files(false);
                var expectedBlobFile = localFiles.stream().map(s -> commit.commitFiles().get(s).blobName()).collect(Collectors.toSet());
                var remoteFiles = blobContainerForCommit.listBlobs(operationPurpose).keySet();
                assertThat(
                    "Expected that all local files " + localFiles + " exist in remote " + remoteFiles,
                    remoteFiles,
                    hasItems(expectedBlobFile.toArray(String[]::new))
                );
                for (String localFile : segmentInfos.files(false)) {
                    BlobLocation blobLocation = commit.commitFiles().get(localFile);
                    final BlobContainer blobContainerForFile = objectStoreService.getBlobContainer(
                        indexShard.shardId(),
                        blobLocation.primaryTerm()
                    );
                    assertThat(localFile, blobContainerForFile.blobExists(operationPurpose, blobLocation.blobName()), is(true));
                    try (
                        IndexInput input = store.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainerForFile.readBlob(
                            operationPurpose,
                            blobLocation.blobName(),
                            blobLocation.offset(),
                            blobLocation.fileLength()
                        );
                    ) {
                        assertEquals("File [" + blobLocation + "] in object store has a different content than local file ", local, remote);
                    }
                }
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
        }
    }

    /**
     * Read the latest BCC from the object store bounded by the given generation (inclusive), i.e. the BCC generation
     * must not be higher than the specified generation.
     */
    private static BatchedCompoundCommit readLatestUploadedBccUptoGen(BlobContainer blobContainerForCommit, long maxGeneration)
        throws IOException {
        final BlobMetadata latestUploadBccMetadata = blobContainerForCommit.listBlobsByPrefix(
            operationPurpose,
            StatelessCompoundCommit.PREFIX
        )
            .values()
            .stream()
            .filter(m -> StatelessCompoundCommit.parseGenerationFromBlobName(m.name()) <= maxGeneration)
            .max(Comparator.comparingLong(m -> StatelessCompoundCommit.parseGenerationFromBlobName(m.name())))
            .orElseThrow(() -> new AssertionError("retry with assertBusy"));
        final var latestUploadedBcc = BatchedCompoundCommit.readFromStore(
            latestUploadBccMetadata.name(),
            latestUploadBccMetadata.length(),
            (blobName, offset, length) -> new InputStreamStreamInput(
                blobContainerForCommit.readBlob(operationPurpose, blobName, offset, length)
            ),
            true
        );
        return latestUploadedBcc;
    }

    private static void assertThatSearchShardIsConsistentWithLastCommit(final IndexShard indexShard, final IndexShard searchShard) {
        final Store indexStore = indexShard.store();
        final Store searchStore = searchShard.store();
        indexStore.incRef();
        searchStore.incRef();
        try {
            ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            // Wait for the latest commit on the index shard is processed on the search engine
            var listener = new SubscribableListener<Long>();
            searchShard.getEngineOrNull()
                .addPrimaryTermAndGenerationListener(
                    indexShard.getOperationPrimaryTerm(),
                    indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration(),
                    listener
                );
            safeAwait(listener);
            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(indexStore.directory());

            // New commit can be uploaded after we read segmentInfos, so we bound the read by the known generation
            final var latestUploadedBcc = readLatestUploadedBccUptoGen(blobContainerForCommit, segmentInfos.getGeneration());
            StatelessCompoundCommit commit = latestUploadedBcc.lastCompoundCommit();
            assertThat(commit.primaryTermAndGeneration().generation(), equalTo(segmentInfos.getGeneration()));

            for (String localFile : segmentInfos.files(false)) {
                var blobPath = commit.commitFiles().get(localFile);
                BlobContainer blobContainer = objectStoreService.getBlobContainer(indexShard.shardId(), blobPath.primaryTerm());
                var blobFile = blobPath.blobName();
                // can take some time for files to be uploaded to the object store
                assertBusy(() -> {
                    assertThat(blobFile, blobContainer.blobExists(operationPurpose, blobFile), is(true));

                    try (
                        IndexInput input = indexStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        IndexInput searchInput = searchStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream searchInputStream = new InputStreamIndexInput(searchInput, searchInput.length());
                    ) {
                        assertEquals(
                            "File [" + blobFile + "] on search shard has a different content than local file ",
                            local,
                            searchInputStream
                        );
                    }
                });
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            indexStore.decRef();
            searchStore.decRef();
        }
    }

    private static void assertEquals(String message, InputStream expected, InputStream actual) throws IOException {
        // adapted from Files.mismatch()
        final int bufferSize = 8192;
        byte[] buffer1 = new byte[bufferSize];
        byte[] buffer2 = new byte[bufferSize];
        try (
            InputStream expectedStream = new BufferedInputStream(expected, bufferSize);
            InputStream actualStream = new BufferedInputStream(actual, bufferSize)
        ) {
            long totalRead = 0;
            while (true) {
                int nRead1 = expectedStream.readNBytes(buffer1, 0, bufferSize);
                int nRead2 = actualStream.readNBytes(buffer2, 0, bufferSize);

                int i = Arrays.mismatch(buffer1, 0, nRead1, buffer2, 0, nRead2);
                assertThat(message + "(position: " + (totalRead + i) + ')', i, equalTo(-1));
                if (nRead1 < bufferSize) {
                    // we've reached the end of the files, but found no mismatch
                    break;
                }
                totalRead += nRead1;
            }
        }
    }

}
