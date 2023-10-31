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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessFileDeletionIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s");
    }

    public void testActiveTranslogFilesArePrunedAfterCommit() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        flush(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
    }

    public void testActiveTranslogFilesNotPrunedOnNotStop() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        var blobContainer = indexObjectStoreService.getTranslogBlobContainer();

        internalCluster().stopNode(indexNode);

        for (TranslogReplicator.BlobTranslogFile translogFile : activeTranslogFiles) {
            assertTrue(blobContainer.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

    public void testActiveTranslogFilesNotPrunedOnFailure() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        var blobContainer = indexObjectStoreService.getTranslogBlobContainer();

        Exception shardFailed = new Exception("Shard Failed");
        if (randomBoolean()) {
            ShardStateAction instance = internalCluster().getInstance(ShardStateAction.class, indexNode);
            PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
            instance.localShardFailed(findIndexShard(indexName).routingEntry(), "test failure", shardFailed, listener);
            listener.actionGet();
        } else {
            internalCluster().getInstance(IndicesService.class, indexNode)
                .getShardOrNull(findIndexShard(indexName).shardId())
                .getEngineOrNull()
                .failEngine("test", shardFailed);
        }

        // Pause to wait async delete complete if it is scheduled
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

        assertThat(translogReplicator.getActiveTranslogFiles().size(), greaterThan(0));

        for (TranslogReplicator.BlobTranslogFile translogFile : activeTranslogFiles) {
            assertTrue(blobContainer.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

    public void testActiveTranslogFilesArePrunedAfterRelocation() throws Exception {
        startMasterOnlyNode();

        int deleteDelayMillis = rarely() ? randomIntBetween(500, 1000) : 0;
        var indexNodeA = startIndexNode(
            Settings.builder().put(OBJECT_STORE_FILE_DELETION_DELAY.getKey(), TimeValue.timeValueMillis(deleteDelayMillis)).build()
        );

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeA);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        long millisBeforeDeletions = System.currentTimeMillis();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodeB), indexName);

        ensureGreen(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
        long millisForDeletions = System.currentTimeMillis() - millisBeforeDeletions;
        assertThat("delete delay should have taken effect", millisForDeletions, greaterThan((long) deleteDelayMillis));
    }

    public void testActiveTranslogFilesArePrunedCaseWithMultipleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexNameA = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String indexNameB = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexNameA,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        createIndex(
            indexNameB,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexNameA, indexNameB);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexNameA, randomIntBetween(1, 100));
            indexDocs(indexNameB, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);
        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        flush(indexNameA);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), greaterThan(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));
        });

        // TODO: Implement the mechanism to allow translog file prune when index deleted
        if (true) {
            flush(indexNameB);
        } else {
            // If meanwhile the index is deleted, we should still be able to clean up the translog blobs, since
            // the other index is committed.
            assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexNameB)).actionGet());
        }

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, objectStoreService);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1066")
    public void testStaleNodeDoesNotDeleteFile() throws Exception {
        String masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNodeA = startIndexNode(
            Settings.builder()
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        SeqNoStats beforeSeqNoStats = client(indexNodeA).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();

        String indexNodeB = startIndexNode();

        ensureStableCluster(3);

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
        final MockTransportService masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        ObjectStoreService indexNodeAObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeA);
        ObjectStoreService indexNodeBObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(indexNodeBObjectStoreService);
        repository.setBlockOnAnyFiles();

        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();

        final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false
                && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                removedNode.onResponse(null);
            }
        });

        try {
            masterTransportService.addUnresponsiveRule(indexNodeTransportService);
            removedNode.actionGet();

            // Slight delay to allow the new node to start recovering from an old commit before the new commit is triggered
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

            client(indexNodeA).admin().indices().prepareFlush(indexName).execute().actionGet();

            assertBusy(() -> {
                assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
                assertThat(translogReplicator.getTranslogFilesToDelete().size(), greaterThan(0));
            });
            assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        } finally {
            masterTransportService.clearAllRules();
        }

        assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
        assertThat(translogReplicator.getTranslogFilesToDelete().size(), greaterThan(0));
        assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        repository.unblock();

        assertBusy(() -> {
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));
            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexNodeAObjectStoreService);
        });

        ensureGreen(indexName);

        SeqNoStats afterSeqNoStats = client(indexNodeB).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        assertEquals(beforeSeqNoStats.getMaxSeqNo(), afterSeqNoStats.getMaxSeqNo());
    }

    public void testCommitsAreRetainedUntilFastRefreshScrollCloses() throws Exception {
        var indexNode = startMasterAndIndexNode();
        var indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        ensureGreen(indexName);

        // awaits #793
        // var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);
        // var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        int totalIndexedDocs = 0;

        var numDocsBeforeOpenScroll = indexDocsAndFlush(indexName);
        totalIndexedDocs += numDocsBeforeOpenScroll;

        // We need to disregard the first empty commit
        // awaits #793
        // var blobsUsedForScroll = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        // must refresh since flush only advances internal searcher.
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        var scrollSearchResponse = client().prepareSearch(indexName)
            .setQuery(matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();

        var numberOfCommitsAfterOpeningScroll = randomIntBetween(3, 5);
        for (int i = 0; i < numberOfCommitsAfterOpeningScroll; i++) {
            totalIndexedDocs += indexDocsAndFlush(indexName);
        }

        // awaits #793
        // var blobsBeforeForceMerge = listBlobsWithAbsolutePath(shardCommitsContainer);

        forceMerge();

        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        // todo: randomly clear cache to go directly to blob store.

        // We request 1 document per search request
        int numberOfScrollRequests = numDocsBeforeOpenScroll - 1;
        for (int i = 0; i < numberOfScrollRequests; i++) {
            var searchResponse = client().prepareSearchScroll(scrollSearchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();
            var hit = searchResponse.getHits().getHits()[0];
            assertThat(hit, is(notNullValue()));
        }

        var searchResponse = client().prepareSearch(indexName).setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalIndexedDocs));

        var indexNodeObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        // awaits #793
        // assertBusy(
        // () -> assertThat(
        // indexNodeObjectStoreService.getCommitBlobsToDelete().stream().noneMatch(blobsUsedForScroll::contains),
        // is(true)
        // )
        // );

        client().prepareClearScroll().addScrollId(scrollSearchResponse.getScrollId()).get();

        // Trigger a new flush so the index shard cleans the unused files after the search node responds with the used commits
        totalIndexedDocs += indexDocsAndFlush(indexName);

        // awaits #793
        // assertBusy(() -> assertThat(indexNodeObjectStoreService.getCommitBlobsToDelete().containsAll(blobsBeforeForceMerge), is(true)));

        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        var finalSearchResponse = client().prepareSearch(indexName).setQuery(matchAllQuery()).get();
        assertThat(finalSearchResponse.getHits().getTotalHits().value, equalTo((long) totalIndexedDocs));
    }

    public void testStaleCommitsArePrunedAfterBeingReleased() throws Exception {
        startMasterOnlyNode();
        int deleteDelayMillis = rarely() ? randomIntBetween(500, 1000) : 0;
        var indexNode = startIndexNode(
            Settings.builder().put(OBJECT_STORE_FILE_DELETION_DELAY.getKey(), TimeValue.timeValueMillis(deleteDelayMillis)).build()
        );
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);

        int totalIndexedDocs = 0;
        int numberOfCommitsBeforeMerge = 3;
        for (int i = 0; i < numberOfCommitsBeforeMerge; i++) {
            totalIndexedDocs += indexDocsAndFlush(indexName);
        }

        var blobsBeforeMerging = listBlobsWithAbsolutePath(shardCommitsContainer);

        long millisBeforeDeletions = System.currentTimeMillis();
        forceMerge();
        // We need to refresh so the local index reader releases the reference from the previous commit
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        assertBusy(() -> {
            var blobsAfterMerging = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsBeforeMerging, blobsAfterMerging), empty());
        });
        long millisForDeletions = System.currentTimeMillis() - millisBeforeDeletions;
        assertThat("delete delay should have taken effect", millisForDeletions, greaterThan((long) deleteDelayMillis));

        var searchResponse = client().prepareSearch(indexName).setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalIndexedDocs));
    }

    public void testCommitsAreRetainedUntilScrollCloses() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_RETAINED_UNTIL_SCROLL_CLOSES);
    }

    public void testCommitsAreDroppedAfterScrollClosesAndIndexingInactivity() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY);
    }

    public void testCommitsOfScrollAreDeletedAfterIndexIsClosedAndOpened() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED);
    }

    public void testAllCommitsDeletedAfterIndexIsDeleted() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.ALL_COMMITS_DELETED_AFTER_INDEX_DELETED);
    }

    private enum TestSearchScrollCase {
        COMMITS_RETAINED_UNTIL_SCROLL_CLOSES,
        COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY,
        COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED,
        ALL_COMMITS_DELETED_AFTER_INDEX_DELETED
    }

    private void testCommitsRetainementWithSearchScroll(TestSearchScrollCase testCase) throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode(
            testCase == TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY
                ? Settings.builder()
                    .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), "100ms")
                    .put(SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), "100ms")
                    .build()
                : Settings.EMPTY
        );
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        int totalIndexedDocs = 0;

        var numDocsBeforeOpenScroll = indexDocsAndFlush(indexName);
        totalIndexedDocs += numDocsBeforeOpenScroll;

        // We need to disregard the first empty commit
        var blobsUsedForScroll = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        var scrollSearchResponse = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();

        var numberOfCommitsAfterOpeningScroll = randomIntBetween(3, 5);
        for (int i = 0; i < numberOfCommitsAfterOpeningScroll; i++) {
            totalIndexedDocs += indexDocsAndFlush(indexName);
        }

        forceMerge();
        // We need to refresh so the local index reader releases the reference from the previous commit
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        // We request 1 document per search request
        int numberOfScrollRequests = numDocsBeforeOpenScroll - 1;
        for (int i = 0; i < numberOfScrollRequests; i++) {
            var searchResponse = client().prepareSearchScroll(scrollSearchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();
            var hit = searchResponse.getHits().getHits()[0];
            assertThat(hit, is(notNullValue()));
        }

        var searchResponse = client().prepareSearch(indexName).setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalIndexedDocs));

        var blobsBeforeReleasingScroll = listBlobsWithAbsolutePath(shardCommitsContainer);
        assertThat(blobsBeforeReleasingScroll.containsAll(blobsUsedForScroll), is(true));

        AtomicInteger countNewCommitNotifications = new AtomicInteger();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                countNewCommitNotifications.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });

        switch (testCase) {
            case COMMITS_RETAINED_UNTIL_SCROLL_CLOSES:
                client().prepareClearScroll().addScrollId(scrollSearchResponse.getScrollId()).get();
                // Trigger a new flush so the index shard cleans the unused files after the search node responds with the used commits
                totalIndexedDocs += indexDocsAndFlush(indexName);
                break;
            case COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY:
                client().prepareClearScroll().addScrollId(scrollSearchResponse.getScrollId()).get();
                // New commit notifications should be sent from the inactive indexing shard so that ultimately the new commit notification
                // responses do not contain the search's open readers anymore, and the shard cleans unused files.
                break;
            case COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED:
                assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());
                client().prepareClearScroll().addScrollId(scrollSearchResponse.getScrollId()).get();
                assertAcked(indicesAdmin().open(new OpenIndexRequest(indexName)).actionGet());
                break;
            case ALL_COMMITS_DELETED_AFTER_INDEX_DELETED:
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
                assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); }); // all blobs should be deleted
                return;
            default:
                assert false : "unknown test case " + testCase;
        }

        // Check that scroll's blobs are deleted
        assertBusy(() -> {
            var blobsAfterReleasingScroll = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsUsedForScroll, blobsAfterReleasingScroll), empty());
        });
        assertThat(countNewCommitNotifications.get(), greaterThan(0));

        if (testCase == TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY) {
            // Verify that there is no more new commit notifications sent
            int currentCount = countNewCommitNotifications.get();
            var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
            indexShardCommitService.runInactivityMonitor(() -> Long.MAX_VALUE);
            assertThat(countNewCommitNotifications.get(), equalTo(currentCount));
        }

        // Check that a new search returns all docs
        var finalSearchResponse = client().prepareSearch(indexName).setQuery(matchAllQuery()).get();
        assertThat(finalSearchResponse.getHits().getTotalHits().value, equalTo((long) totalIndexedDocs));
    }

    public void testDeleteIndexAfterFlush() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);
        indexDocsAndFlush(indexName);
        assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
        assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); });
    }

    public void testDeleteIndexAfterRecovery() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        final var searchNodeA = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var totalIndexedDocs = indexDocsAndFlush(indexName);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNodeA);

        startIndexNode();
        startSearchNode();
        final var excludeIndexOrSearchNode = randomBoolean();
        String nodeToExclude = excludeIndexOrSearchNode ? indexNodeA : searchNodeA;
        boolean excludeOrStop = randomBoolean();
        logger.info(
            "--> {} {} node {}",
            excludeOrStop ? "excluding" : "stopping",
            excludeIndexOrSearchNode ? "index" : "search",
            nodeToExclude
        );
        if (excludeOrStop) {
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeToExclude), indexName);
            if (randomBoolean()) {
                assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodeToExclude))));
            }
        } else {
            internalCluster().stopNode(nodeToExclude);
        }

        logger.info("--> deleting index");
        assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
        if (excludeIndexOrSearchNode && excludeOrStop) {
            // TODO: ES-6897 we cannot assert all blobs are deleted because the primary relocation handoff has leftover files
        } else {
            assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); }); // all blobs should be deleted
        }
    }

    public void testStaleNodeDoesNotDeleteCommitFiles() throws Exception {
        startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNodeA = startIndexNode(
            Settings.builder()
                // This prevents triggering an election in the isolated node once the link between it and the master is blackholed
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "200")
                .build()
        );
        ensureStableCluster(2);

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNodeA);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        final int numberOfSegments = randomIntBetween(2, 5);
        for (int i = 0; i < numberOfSegments; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }

        var indexNodeB = startIndexNode();
        ensureStableCluster(3);

        var shardId = new ShardId(resolveIndex(indexName), 0);

        // We need to disregard the first empty commit that's deleted right away
        var blobsBeforeTriggeringForceMerge = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
        final MockTransportService masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());

        var primaryShardNodeRemoved = new PlainActionFuture<>();
        var shardRelocated = new PlainActionFuture<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(clusterChangedEvent -> {
            if (primaryShardNodeRemoved.isDone() == false
                && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                primaryShardNodeRemoved.onResponse(null);
            }
            if (shardRelocated.isDone() == false) {
                var clusterState = clusterChangedEvent.state();
                var primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
                if (primaryShard.started() && primaryShard.currentNodeId().equals(clusterState.nodes().resolveNode(indexNodeB).getId())) {
                    shardRelocated.onResponse(null);
                }
            }
        });

        var rejoinedCluster = new PlainActionFuture<>();
        internalCluster().getInstance(ClusterService.class, indexNodeA).addListener(clusterChangedEvent -> {
            if (rejoinedCluster.isDone() == false) {
                var nodesDelta = clusterChangedEvent.nodesDelta();
                if (nodesDelta.masterNodeChanged() && nodesDelta.previousMasterNode() == null) {
                    rejoinedCluster.onResponse(null);
                }
            }
        });

        masterTransportService.addUnresponsiveRule(indexNodeTransportService);
        primaryShardNodeRemoved.actionGet();

        // Trigger a force merge in the stale primary to force a "possible" deletion of the previous commits
        var forceMergeFuture = client(indexNodeA).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();

        // Ensure that the shard is relocated to indexNodeB
        shardRelocated.get();

        // The consistency check reads the blob store and notices that it's behind and waits until there's a new cluster state update
        masterTransportService.clearAllRules();

        forceMergeFuture.get();
        // The consistency check is executed in an observer that's applied before the listener that triggers this future
        rejoinedCluster.get();

        var blobsAfterNodeIsStale = listBlobsWithAbsolutePath(shardCommitsContainer);
        assertThat(blobsAfterNodeIsStale.containsAll(blobsBeforeTriggeringForceMerge), is(true));
    }

    // Since the commit deletion relies on a NewCommitNotification being process on all unpromotables, while an unpromtable is
    // recovering (and cannot process a NewCommitNotification), commits should not be deleted.
    public void testCommitsNotDeletedWhileAnUnpromotableIsRecovering() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(indexName);
        var searchNode2 = startSearchNode();
        ensureStableCluster(3);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);
        // Create some commits
        int commits = randomIntBetween(2, 5);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
        }
        CountDownLatch commitRegistrationStarted = new CountDownLatch(1);
        CountDownLatch newCommitCreated = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode2).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                commitRegistrationStarted.countDown();
                safeAwait(newCommitCreated);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // Start the second search shard
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2), indexName);
        safeAwait(commitRegistrationStarted);
        // While search shard is recovering, create new commits by merge/refresh which should normally delete older commits
        var blobsBeforeMerge = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);
        forceMerge();
        client().admin().indices().prepareRefresh(indexName).execute().get();
        var blobsAfterMergeAndRefresh = listBlobsWithAbsolutePath(shardCommitsContainer);
        // No deletion should happen since there is a RECOVERING unpromotable
        assertThat(
            "blobs before merge = " + blobsBeforeMerge + ", blobs after merge and refresh=" + blobsAfterMergeAndRefresh,
            blobsAfterMergeAndRefresh.containsAll(blobsBeforeMerge),
            is(true)
        );
        // Allow recovery to finish and verify that the commits are deleted.
        newCommitCreated.countDown();
        ensureGreen(indexName);
        // We need a new commit to trigger file deletion
        indexDocsAndFlush(indexName);
        assertBusy(() -> {
            var blobsAfterRecoveryAndRefresh = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsAfterRecoveryAndRefresh, blobsBeforeMerge), is(empty()));
        });
    }

    private Set<String> listBlobsWithAbsolutePath(BlobContainer blobContainer) throws IOException {
        var blobContainerPath = blobContainer.path().buildAsString();
        return blobContainer.listBlobs(operationPurpose)
            .keySet()
            .stream()
            .map(blob -> blobContainerPath + blob)
            .collect(Collectors.toSet());
    }

    private static BlobContainer getShardCommitsContainerForCurrentPrimaryTerm(String indexName, String indexNode) {
        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        var primaryTerm = client().admin().cluster().prepareState().get().getState().metadata().index(indexName).primaryTerm(0);
        var shardId = new ShardId(resolveIndex(indexName), 0);
        return indexObjectStoreService.getBlobContainer(shardId, primaryTerm);
    }

    private int indexDocsAndFlush(String indexName) {
        int numDocsBeforeOpenScroll = randomIntBetween(10, 20);
        indexDocs(indexName, numDocsBeforeOpenScroll);
        flush(indexName);
        return numDocsBeforeOpenScroll;
    }

    private static void assertTranslogBlobsExist(
        Set<TranslogReplicator.BlobTranslogFile> shouldExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(objectStoreService.getTranslogBlobContainer().blobExists(operationPurpose, translogFile.blobName()));
        }
    }

    private static void assertTranslogBlobsDoNotExist(
        Set<TranslogReplicator.BlobTranslogFile> doNotExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : doNotExist) {
            assertFalse(objectStoreService.getTranslogBlobContainer().blobExists(operationPurpose, translogFile.blobName()));
        }
    }
}
