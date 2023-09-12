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

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // TODO: remove heartbeat setting once ES-6481 is done. It is currently needed for testOngoingIndexShardRelocationAndMasterFailOver.
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(5))
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s");
    }

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    private void testTranslogRecovery(boolean heavyIndexing) throws Exception {
        startIndexNodes(2);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );
        ensureGreen(indexName);

        if (heavyIndexing) {
            indexDocuments(indexName); // produces several commits
            indexDocs(indexName, randomIntBetween(50, 100));
        } else {
            indexDocs(indexName, randomIntBetween(1, 5));
        }

        // The following custom documents will exist in translog and not committed before the node restarts.
        // After the node restarts, we can search for them to check that they exist.
        int customDocs = randomIntBetween(1, 5);
        int baseId = randomIntBetween(200, 300);
        for (int i = 0; i < customDocs; i++) {
            index(indexName, String.valueOf(baseId + i), Map.of("custom", "value"));
        }

        // Assert that the seqno before and after restarting the indexing node is the same
        SeqNoStats beforeSeqNoStats = client().admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        Index index = resolveIndices().keySet().stream().filter(i -> i.getName().equals(indexName)).findFirst().get();
        DiscoveryNode node = findIndexNode(index, 0);
        internalCluster().restartNode(node.getName());
        ensureGreen(indexName);
        SeqNoStats afterSeqNoStats = client().admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        assertEquals(beforeSeqNoStats, afterSeqNoStats);

        // Assert that the custom documents added above are returned when searched
        startSearchNodes(1);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).get();
        assertHitCount(searchResponse, customDocs);
    }

    public void testTranslogRecoveryWithHeavyIndexing() throws Exception {
        testTranslogRecovery(true);
    }

    public void testTranslogRecoveryWithLightIndexing() throws Exception {
        testTranslogRecovery(false);
    }

    public void testRelocatingIndexShards() throws Exception {
        final var numShards = randomIntBetween(1, 3);
        final var indexNodes = startIndexNodes(Math.max(2, numShards));

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);

        final ClusterStateListener verifyGreenListener = event -> {
            // ensure that the master remains unchanged, and the index remains green, throughout the test
            assertTrue(event.localNodeMaster());
            final var indexRoutingTable = event.state().routingTable().index(indexName);
            assertEquals(numShards, indexRoutingTable.size());
            for (int i = 0; i < numShards; i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertEquals(1, indexShardRoutingTable.size());
                assertThat(
                    indexRoutingTable.prettyPrint(),
                    indexShardRoutingTable.primaryShard().state(),
                    oneOf(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING)
                );
            }
        };

        final var masterNodeClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterNodeClusterService.addListener(verifyGreenListener);

        try {
            final AtomicInteger docIdGenerator = new AtomicInteger();
            final IntConsumer docIndexer = numDocs -> {
                var bulkRequest = client().prepareBulk();
                for (int i = 0; i < numDocs; i++) {
                    bulkRequest.add(
                        new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                            .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                    );
                }
                assertNoFailures(bulkRequest.get(TimeValue.timeValueSeconds(10)));
            };

            docIndexer.accept(between(1, 100));
            if (randomBoolean()) {
                flush(indexName);
            }

            final var initialPrimaryTerms = getPrimaryTerms(indexName);

            final int iters = randomIntBetween(5, 10);
            for (int i = 0; i < iters; i++) {

                final var nodeToRemove = indexNodes.get(i % indexNodes.size());

                final AtomicBoolean running = new AtomicBoolean(true);

                final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
                for (int j = 0; j < threads.length; j++) {
                    threads[j] = new Thread(() -> {
                        while (running.get()) {
                            docIndexer.accept(between(1, 20));
                        }
                    });
                    threads[j].start();
                }

                try {
                    logger.info("--> excluding [{}]", nodeToRemove);
                    updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeToRemove), indexName);
                    assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodeToRemove))));
                } finally {
                    running.set(false);
                    for (Thread thread : threads) {
                        thread.join();
                    }
                }

                assertArrayEquals(initialPrimaryTerms, getPrimaryTerms(indexName));

                for (String indexNode : indexNodes) {
                    assertNodeHasNoCurrentRecoveries(indexNode);
                }

                if (randomBoolean()) {
                    docIndexer.accept(between(1, 10));
                }

                // verify all docs are present without needing input from a search node
                var bulkRequest = client().prepareBulk();
                for (int docId = 1; docId < docIdGenerator.get(); docId++) {
                    bulkRequest.add(new IndexRequest(indexName).id("doc-" + docId).create(true).source(Map.of()));
                }
                var bulkResponse = bulkRequest.get(TimeValue.timeValueSeconds(10));
                for (BulkItemResponse bulkResponseItem : bulkResponse.getItems()) {
                    assertEquals(RestStatus.CONFLICT, bulkResponseItem.status());
                }
            }
        } finally {
            masterNodeClusterService.removeListener(verifyGreenListener);
        }
    }

    /**
     * Verify that if we index after a relocation, we remember the indexed ops even if the new node crashes.
     * This ensures that there is a flush with a new translog registration after relocation.
     */
    public void testIndexAfterRelocation() throws IOException {
        final var numShards = randomIntBetween(1, 3);
        final var indexNode = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);
        final AtomicInteger docIdGenerator = new AtomicInteger();
        final IntConsumer docIndexer = numDocs -> {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < numDocs; i++) {
                bulkRequest.add(
                    new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                        .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                );
            }
            assertNoFailures(bulkRequest.get(TimeValue.timeValueSeconds(10)));
        };

        docIndexer.accept(between(1, 10));

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode), indexName);

        final var indexNode2 = startIndexNode();

        // wait for relocation
        ensureGreen();

        docIndexer.accept(between(1, 10));

        // we ought to crash, but do not flush on close in stateless
        internalCluster().stopNode(indexNode2);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", (String) null), indexName);
        ensureGreen();

        // verify all docs are present without needing input from a search node
        var bulkRequest = client().prepareBulk();
        for (int docId = 1; docId < docIdGenerator.get(); docId++) {
            bulkRequest.add(new IndexRequest(indexName).id("doc-" + docId).create(true).source(Map.of()));
        }
        var bulkResponse = bulkRequest.get(TimeValue.timeValueSeconds(10));
        for (BulkItemResponse bulkResponseItem : bulkResponse.getItems()) {
            assertEquals(RestStatus.CONFLICT, bulkResponseItem.status());
        }
    }

    public void testFailedRelocatingIndexShardHasNoCurrentRecoveries() throws Exception {
        final var indexNodeA = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var indexNodeB = startIndexNode();
        ensureStableCluster(3); // with master node

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);

        logger.info("--> excluding [{}]", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        // The shard fails to be relocated to indexNodeB and finally stays green on indexNodeA
        ensureGreen(indexName);
        assertNodeHasNoCurrentRecoveries(indexNodeA);
        assertNodeHasNoCurrentRecoveries(indexNodeB);
    }

    private long[] getPrimaryTerms(String indexName) {
        return getPrimaryTerms(client(), indexName);
    }

    private static long[] getPrimaryTerms(Client client, String indexName) {
        var response = client.admin().cluster().prepareState().get();
        var state = response.getState();

        var indexMetadata = state.metadata().index(indexName);
        long[] primaryTerms = new long[indexMetadata.getNumberOfShards()];
        for (int i = 0; i < primaryTerms.length; i++) {
            primaryTerms[i] = indexMetadata.primaryTerm(i);
        }
        return primaryTerms;
    }

    public void testRelocateNonexistentIndexShard() throws Exception {
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNodes.get(0));
        final var delayedRequestFuture = new PlainActionFuture<Runnable>();
        final var delayedRequestFutureOnce = ActionListener.assertOnce(delayedRequestFuture);
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> delayedRequestFutureOnce.onResponse(
                () -> ActionListener.run(new ChannelActionListener<>(channel), l -> handler.messageReceived(request, channel, task))
            )
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);

        assertNotNull(delayedRequestFuture.get(10, TimeUnit.SECONDS));
        transportService.clearInboundRules();

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var masterServiceBarrier = new CyclicBarrier(2);
        masterClusterService.submitUnbatchedStateUpdateTask("blocking", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                safeAwait(masterServiceBarrier);
                safeAwait(masterServiceBarrier);
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        safeAwait(masterServiceBarrier); // wait for master service to be blocked, so the shard cannot be reallocated after failure

        final var index = masterClusterService.state().metadata().index(indexName).getIndex();
        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNodes.get(0));
        final var indexShard = indicesService.indexService(index).getShard(0);
        indexShard.failShard("test", new ElasticsearchException("test"));
        assertBusy(() -> assertNull(indicesService.getShardOrNull(indexShard.shardId())));

        delayedRequestFuture.get().run(); // release relocation request which will fail because the shard is no longer there
        safeAwait(masterServiceBarrier); // release master service to restart allocation process

        ensureGreen(indexName);
    }

    public void testRetryIndexShardRelocation() throws Exception {
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNodes.get(0));
        final var allAttemptsFuture = new PlainActionFuture<Void>();
        final var attemptListener = new CountDownActionListener(
            MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY),
            allAttemptsFuture
        );
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> ActionListener.completeWith(attemptListener, () -> {
                channel.sendResponse(new ElasticsearchException("simulated"));
                return null;
            })
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);
        allAttemptsFuture.get(10, TimeUnit.SECONDS);

        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(0)), internalCluster().nodesInclude(indexName));

        internalCluster().stopNode(indexNodes.get(0));
        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(1)), internalCluster().nodesInclude(indexName));
    }

    public void testFailureAfterPrimaryContextHandoff() throws Exception {
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNodes.get(0));
        final var allAttemptsFuture = new PlainActionFuture<Void>();
        final var attemptListener = new CountDownActionListener(1, allAttemptsFuture); // to assert that there's only one attempt
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> ActionListener.run(
                new ChannelActionListener<>(channel).<TransportResponse>delegateFailure((l, r) -> {
                    attemptListener.onResponse(null);
                    l.onFailure(new ElasticsearchException("simulated"));
                }),
                l -> handler.messageReceived(request, new TestTransportChannel(l), task)
            )
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);
        allAttemptsFuture.get(10, TimeUnit.SECONDS);

        // the failure happens after the primary context handoff, so it causes both copies to fail, and then the primary initializes from
        // scratch on the correct node
        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(1)), internalCluster().nodesInclude(indexName));
    }

    public void testRecoverIndexingShard() throws Exception {

        var indexingNode1 = startIndexNode();
        startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int numDocsRound1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound1);
        refresh(indexName);

        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1);

        if (randomBoolean()) {
            internalCluster().restartNode(indexingNode1);
        } else {
            internalCluster().stopNode(indexingNode1);
            startIndexNode(); // replacement node
        }

        ensureGreen(indexName);

        int numDocsRound2 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound2);
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1 + numDocsRound2);
    }

    public void testRecoverSearchShard() throws IOException {

        startIndexNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));

        var searchNode1 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode1);

        var searchNode2 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode2);
    }

    public void testRecoverMultipleIndexingShardsWithCoordinatingRetries() throws Exception {
        String firstIndexingShard = startIndexNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);

        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            firstIndexingShard
        );

        transportService.addRequestHandlingBehavior(
            TransportShardBulkAction.ACTION_NAME,
            (handler, request, channel, task) -> handler.messageReceived(request, new TestTransportChannel(ActionListener.noop()), task)
        );

        String coordinatingNode = startIndexNode();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", coordinatingNode), indexName);

        ActionFuture<BulkResponse> bulkRequest = client(coordinatingNode).prepareBulk(indexName)
            .add(new IndexRequest(indexName).source(Map.of("custom", "value")))
            .execute();

        assertBusy(() -> {
            IndicesStatsResponse statsResponse = client(firstIndexingShard).admin().indices().prepareStats(indexName).get();
            SeqNoStats seqNoStats = statsResponse.getIndex(indexName).getShards()[0].getSeqNoStats();
            assertThat(seqNoStats.getMaxSeqNo(), equalTo(0L));
        });
        flush(indexName);

        internalCluster().stopNode(firstIndexingShard);

        String secondIndexingShard = startIndexNode();
        ensureGreen(indexName);

        BulkResponse response = bulkRequest.actionGet();
        assertFalse(response.hasFailures());

        internalCluster().stopNode(secondIndexingShard);

        startIndexNodes(1);
        ensureGreen(indexName);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        startSearchNode();
        ensureGreen(indexName);

        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).get();
        assertHitCount(searchResponse, 1);
    }

    public void testStartingTranslogFileWrittenInCommit() throws Exception {
        List<String> indexNodes = startIndexNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodes.get(0));
        Map<String, BlobMetadata> translogFiles = objectStoreService.getTranslogBlobContainer().listBlobs();

        final String newIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            newIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(newIndex);

        Index index = resolveIndex(newIndex);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        String commitFile = blobNameFromGeneration(Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration());
        assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );

        long initialRecoveryCommitStartingFile = commit.translogRecoveryStartFile();

        // Greater than or equal to because translog files start at 0
        assertThat(initialRecoveryCommitStartingFile, greaterThanOrEqualTo((long) translogFiles.size()));

        indexDocs(newIndex, randomIntBetween(1, 5));

        flush(newIndex);

        commitFile = blobNameFromGeneration(Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration());
        assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );

        // Recovery file has advanced because of flush
        assertThat(commit.translogRecoveryStartFile(), greaterThan(initialRecoveryCommitStartingFile));
    }

    public void testRerouteRecoveryOfIndexShard() throws Exception {
        final String nodeA = startIndexNode();
        logger.info("--> started index node A [{}]", nodeA);

        logger.info("--> create index on node: {}", nodeA);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(1000, 1500));
        refresh(indexName);

        final String nodeB = startIndexNode();
        ensureGreen();
        logger.info("--> started index node B [{}]", nodeB);

        logger.info("--> blocking recoveries on " + nodeB);
        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, nodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setBlockOnAnyFiles();

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, nodeA, nodeB)).execute().actionGet().getState();

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(indexName);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        logger.info("--> request node recovery stats");
        NodesStatsResponse statsResponse = clusterAdmin().prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().getName().equals(nodeA)) {
                assertThat("node A should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat("node A should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
            }
            if (nodeStats.getNode().getName().equals(nodeB)) {
                assertThat("node B should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat("node B should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
            }
        }

        logger.info("--> unblocking recoveries on " + nodeB);
        repository.unblock();

        // wait for it to be finished
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();

        recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(
            recoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeA));
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeB));
    }

    public void testRerouteRecoveryOfSearchShard() throws Exception {
        final String nodeA = startIndexNode();
        logger.info("--> started index node A [{}]", nodeA);

        logger.info("--> create index on node: {}", nodeA);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(1000, 1500));
        refresh(indexName);

        String nodeB = startSearchNode();
        ensureGreen();
        logger.info("--> started search node B [{}]", nodeB);
        logger.info("--> bump replica count");
        setReplicaCount(1, indexName);
        ensureGreen();

        String nodeC = startSearchNode();
        ensureGreen();
        logger.info("--> started search node C [{}]", nodeC);

        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeA));
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeB));
        assertFalse(clusterAdmin().prepareHealth().setWaitForNodes("4").get().isTimedOut()); // including master node

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, nodeC);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        logger.info("--> block recoveries on " + nodeC);
        repository.setBlockOnAnyFiles();

        logger.info("--> move replica shard from: {} to: {}", nodeB, nodeC);
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, nodeB, nodeC)).execute().actionGet().getState();

        RecoveryResponse response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);

        assertThat(nodeARecoveryStates.size(), equalTo(1));
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(
            nodeBRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown nodeB and check if recovery continues
            internalCluster().stopNode(nodeB);
            ensureStableCluster(3);

            response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
            recoveryStates = response.shardRecoveryStates().get(indexName);

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates.size(), equalTo(1));
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates.size(), equalTo(0));
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates.size(), equalTo(1));

            assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
            validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
        }

        logger.info("--> unblocking recoveries on " + nodeC);
        repository.unblock();
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(indexName);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(0));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeCRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeC
        );
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    public void testRecoveryMarksNewNodeInCommit() throws Exception {
        String initialNode = startIndexNodes(1).get(0);
        startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );
        ensureGreen(indexName);

        int numDocsRound1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound1);
        refresh(indexName);

        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1);

        internalCluster().stopNode(initialNode);
        // second replacement node. we are checking here that green state == flush occurred so that the third node recovers from the correct
        // commit which will reference the buffered translog operations written on the second node
        String secondNode = startIndexNode();

        ensureGreen(indexName);

        int numDocsRound2 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound2);

        internalCluster().stopNode(secondNode);
        startIndexNode(); // third replacement node
        ensureGreen(indexName);

        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1 + numDocsRound2);
    }

    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        List<RecoveryState> nodeResponses = new ArrayList<>();
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(recoveryState);
            }
        }
        return nodeResponses;
    }

    private void assertRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource type,
        boolean primary,
        RecoveryState.Stage stage,
        String sourceNode,
        String targetNode
    ) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource type,
        boolean primary,
        String sourceNode,
        String targetNode
    ) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), not(equalTo(RecoveryState.Stage.DONE)));
    }

    private void assertRecoveryStateWithoutStage(
        RecoveryState state,
        int shardId,
        RecoverySource recoverySource,
        boolean primary,
        String sourceNode,
        String targetNode
    ) {
        assertThat(state.getShardId().getId(), equalTo(shardId));
        assertThat(state.getRecoverySource(), equalTo(recoverySource));
        assertThat(state.getPrimary(), equalTo(primary));
        if (sourceNode == null) {
            assertNull(state.getSourceNode());
        } else {
            assertNotNull(state.getSourceNode());
            assertThat(state.getSourceNode().getName(), equalTo(sourceNode));
        }
        if (targetNode == null) {
            assertNull(state.getTargetNode());
        } else {
            assertNotNull(state.getTargetNode());
            assertThat(state.getTargetNode().getName(), equalTo(targetNode));
        }
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    private void assertNodeHasNoCurrentRecoveries(String nodeName) {
        NodesStatsResponse nodesStatsResponse = clusterAdmin().prepareNodesStats()
            .setNodesIds(nodeName)
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        assertThat(nodesStatsResponse.getNodes(), hasSize(1));
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
        assertThat(recoveryStats.currentAsSource(), equalTo(0));
        assertThat(recoveryStats.currentAsTarget(), equalTo(0));
    };

    public void testRecoverIndexingShardWithObjectStoreFailuresDuringIndexing() throws Exception {
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeA);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
        // This pattern starts failing from file 10. Because file name has 19 digits, and final digit should not be preceded by 18 zeroes.
        repository.setRandomIOExceptionPattern(".*translog/\\d{18,18}(?<!000000000000000000)\\d.*");

        final AtomicInteger docIdGenerator = new AtomicInteger();
        final AtomicInteger docsAcknowledged = new AtomicInteger();
        final AtomicInteger docsFailed = new AtomicInteger();
        final IntConsumer docIndexer = numDocs -> {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < numDocs; i++) {
                bulkRequest.add(
                    new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                        .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                );
            }
            BulkResponse response = bulkRequest.get(TimeValue.timeValueSeconds(15));
            assertThat(response.getItems().length, equalTo(numDocs));
            for (BulkItemResponse itemResponse : response.getItems()) {
                if (itemResponse.isFailed()) {
                    docsFailed.incrementAndGet();
                } else {
                    docsAcknowledged.incrementAndGet();
                }
            }
        };

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(() -> {
                while (running.get()) {
                    docIndexer.accept(between(1, 20));
                }
            });
            threads[j].start();
        }

        try {
            assertBusy(() -> assertThat(repository.getFailureCount(), greaterThan(0L)));
        } finally {
            running.set(false);
            internalCluster().stopNode(indexNodeA);
            for (Thread thread : threads) {
                thread.join();
            }
        }

        logger.info("--> [{}] documents acknowledged, [{}] documents failed", docsAcknowledged, docsFailed);
        ensureGreen();

        refresh(indexName); // so that any translog ops become visible for searching
        final long totalHits = client().prepareSearch(indexName).get().getHits().getTotalHits().value;
        assertThat(totalHits, greaterThanOrEqualTo((long) docsAcknowledged.get()));
    }

    public void testRecoverIndexingShardWithStaleIndexingShard() throws Exception {
        String indexNodeA = startIndexNode();
        String searchNode = startSearchNode();
        String masterName = internalCluster().getMasterName();

        final String indexName = "index-name";
        createIndex(indexName, indexSettings(1, 1).put("index.unassigned.node_left.delayed_timeout", "0ms").build());
        ensureGreen(indexName);

        final AtomicInteger docIdGenerator = new AtomicInteger();
        final AtomicInteger docsAcknowledged = new AtomicInteger();
        final AtomicInteger docsFailed = new AtomicInteger();

        var bulkRequest = client(indexNodeA).prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                    .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
            );
        }

        // Index some operations
        for (BulkItemResponse itemResponse : bulkRequest.get().getItems()) {
            if (itemResponse.isFailed()) {
                docsFailed.incrementAndGet();
            } else {
                docsAcknowledged.incrementAndGet();
            }
        }

        MockTransportService nodeATransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            indexNodeA
        );
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            masterName
        );

        String indexNodeB = startIndexNode();

        ensureStableCluster(4);

        long initialPrimaryTerm = getPrimaryTerms(indexName)[0];

        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        final PlainActionFuture<Void> staleRequestDone = new PlainActionFuture<>();
        try {
            final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
            masterClusterService.addListener(clusterChangedEvent -> {
                if (removedNode.isDone() == false
                    && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                    removedNode.onResponse(null);
                }
            });
            masterTransportService.addUnresponsiveRule(nodeATransportService);

            removedNode.actionGet();

            logger.info("waiting for [{}] to be removed from cluster", indexNodeA);
            ensureStableCluster(3, masterName);

            assertBusy(() -> assertThat(getPrimaryTerms(client(masterName), indexName)[0], greaterThan(initialPrimaryTerm)));

            ClusterHealthRequest healthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(30))
                .waitForStatus(ClusterHealthStatus.GREEN)
                .waitForEvents(Priority.LANGUID)
                .waitForNoRelocatingShards(true)
                .waitForNoInitializingShards(true)
                .waitForNodes(Integer.toString(3));

            client(randomFrom(indexNodeB, searchNode)).admin().cluster().health(healthRequest).actionGet();

            var staleBulkRequest = client(indexNodeA).prepareBulk();
            for (int i = 0; i < 10; i++) {
                staleBulkRequest.add(
                    new IndexRequest(indexName).id("stale-doc-" + docIdGenerator.incrementAndGet())
                        .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                );
            }
            staleBulkRequest.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
                        if (itemResponse.isFailed()) {
                            docsFailed.incrementAndGet();
                        } else {
                            docsAcknowledged.incrementAndGet();
                        }
                    }
                    staleRequestDone.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    staleRequestDone.onFailure(e);
                }
            });

            // Slight delay to allow the stale node to potentially process requests before healing
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        } finally {
            masterTransportService.clearAllRules();
        }

        logger.info("--> [{}] documents acknowledged, [{}] documents failed", docsAcknowledged, docsFailed);
        ensureGreen(indexName);

        staleRequestDone.actionGet();

        refresh(indexName);
        final long totalHits = client().prepareSearch(indexName).get().getHits().getTotalHits().value;
        assertThat(totalHits, equalTo((long) docsAcknowledged.get()));
    }

    public void testRecoverSearchShardWithObjectStoreFailures() throws Exception {
        final String indexName = "test";
        startIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(25, 250);
        indexDocsAndRefresh(indexName, numDocs);
        ensureSearchable(indexName);

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, searchNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) repository.setRandomIOExceptionPattern(".*stateless_commit_.*");

        logger.info("--> starting search shard");
        setReplicaCount(1, indexName);

        ensureGreen();
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
    }

    public void testRelocateSearchShardWithObjectStoreFailures() throws Exception {
        final String indexName = "test";
        startIndexNode();
        final String searchNodeA = startSearchNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 1).build());
        int numDocs = scaledRandomIntBetween(25, 250);
        indexDocsAndRefresh(indexName, numDocs);
        final String searchNodeB = startSearchNode();
        ensureStableCluster(4);

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, searchNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) repository.setRandomIOExceptionPattern(".*stateless_commit_.*");

        logger.info("--> move replica shard from: {} to: {}", searchNodeA, searchNodeB);
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, searchNodeA, searchNodeB)).execute().actionGet();

        ensureGreen();
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertNodeHasNoCurrentRecoveries(searchNodeB);
        final String searchNodeBId = internalCluster().getInstance(ClusterService.class, searchNodeB).localNode().getId();
        assertThat(findSearchShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(searchNodeBId));
        assertHitCount(client(searchNodeB).prepareSearch(indexName).setPreference("_local").get(), numDocs);
    }

    public void testRecoverIndexingShardWithObjectStoreFailures() throws Exception {
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*stateless_commit_.*");
        } else if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*translog.*");
        }

        logger.info("--> stopping node [{}]", indexNodeA);
        internalCluster().stopNode(indexNodeA);
        ensureStableCluster(2);

        ensureGreen();
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    public void testRelocateIndexingShardWithObjectStoreFailures() throws Exception {
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        boolean failuresOnSource = randomBoolean(); // else failures on target node
        logger.info("--> failures will be on source node? [{}]", failuresOnSource);
        ObjectStoreService objectStoreService = internalCluster().getInstance(
            ObjectStoreService.class,
            failuresOnSource ? indexNodeA : indexNodeB
        );
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) {
            logger.info("--> failures will be on stateless commits");
            repository.setRandomIOExceptionPattern(".*stateless_commit_.*");
        } else if (failuresOnSource == false && randomBoolean()) {
            // source node does not do anything with the translog at this point
            // target node lists the translog blobs, but skips them (since the translog recovery start file is greater than what it lists)
            logger.info("--> failures will be on translog files");
            repository.setRandomIOExceptionPattern(".*translog.*");
        }

        logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB)).execute().actionGet();

        ensureGreen();
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    public void testIndexShardRecoveryDoesNotUseTranslogOperationsBeforeFlush() throws Exception {
        final String indexNodeA = startIndexNode();

        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> client().prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = internalCluster().getInstance(
            IndicesService.class,
            discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()
        ).getShardOrNull(shardId);
        final long maxSeqNoBeforeFlush = primary.seqNoStats().getMaxSeqNo();
        assertBusy(() -> assertThat(primary.getLastSyncedGlobalCheckpoint(), equalTo(maxSeqNoBeforeFlush)));
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), is(0));

        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> client().prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        final long maxSeqNoAfterFlush = primary.seqNoStats().getMaxSeqNo();
        logger.info("--> stopping node {} in order to re-allocate indexing shard on node {}", indexNodeA, indexNodeB);
        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(RecoveryState::getPrimary)
            .findFirst()
            .get();
        assertThat((long) recoveryState.getTranslog().recoveredOperations(), lessThanOrEqualTo(maxSeqNoAfterFlush - maxSeqNoBeforeFlush));
    }

    public void testRelocateIndexingShardWithActionFailures() throws Exception {
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        String actionToBreak = randomBoolean() ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
        final CountDownLatch requestFailed = startBreakingActions(indexNodeA, indexNodeB, actionToBreak);

        logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB)).execute().actionGet();

        assertTrue(requestFailed.await(30, TimeUnit.SECONDS));
        stopBreakingActions(indexNodeA, indexNodeB);

        ensureGreen();
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    private CountDownLatch startBreakingActions(String nodeA, String nodeB, String recoveryActionToBlock) throws Exception {
        logger.info("--> will break requests between node [{}] & node [{}] for actions [{}]", nodeA, nodeB, recoveryActionToBlock);

        MockTransportService nodeAMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodeA
        );
        MockTransportService nodeBMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodeB
        );
        TransportService nodeATransportService = internalCluster().getInstance(TransportService.class, nodeA);
        TransportService nodeBTransportService = internalCluster().getInstance(TransportService.class, nodeB);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            final StubbableTransport.SendRequestBehavior sendRequestBehavior = (connection, requestId, action, request, options) -> {
                if (recoveryActionToBlock.equals(action)) {
                    requestFailed.countDown();
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
                connection.sendRequest(requestId, action, request, options);
            };
            // Fail on the sending side
            nodeAMockTransportService.addSendBehavior(nodeBTransportService, sendRequestBehavior);
            nodeBMockTransportService.addSendBehavior(nodeATransportService, sendRequestBehavior);
        } else {
            // Fail on the receiving side.
            nodeAMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                nodeBMockTransportService.disconnectFromNode(nodeAMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
            nodeBMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                nodeAMockTransportService.disconnectFromNode(nodeBMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
        }

        return requestFailed;
    }

    private void stopBreakingActions(String... nodes) throws Exception {
        for (String node : nodes) {
            MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mockTransportService.clearAllRules();
        }
        logger.info("--> stopped breaking requests on nodes [{}]", Strings.collectionToCommaDelimitedString(Arrays.stream(nodes).toList()));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/776")
    public void testOngoingIndexShardRelocationAndMasterFailOver() throws Exception {
        String indexName = "test";
        startMasterOnlyNode(); // second master eligible node
        final String indexNodeA = startIndexNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);
        final String indexNodeB = startIndexNode();
        ensureStableCluster(4);
        final boolean blockSourceNode = randomBoolean(); // else block target node

        final String nodeToBlock = blockSourceNode ? indexNodeA : indexNodeB;
        MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeToBlock);
        final SubscribableListener<Void> blockedListeners = new SubscribableListener<>();
        CountDownLatch relocationStartReadyBlocked = new CountDownLatch(1);
        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            final String actionToBlock = blockSourceNode ? PRIMARY_CONTEXT_HANDOFF_ACTION_NAME : START_RELOCATION_ACTION_NAME;
            if (actionToBlock.equals(action)) {
                logger.info("--> Blocking the action [{}]", action);
                blockedListeners.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        try {
                            logger.info("--> unblocking request [{}][{}][{}]", requestId, action, request);
                            connection.sendRequest(requestId, action, request, options);
                        } catch (NodeNotConnectedException e) {
                            logger.info("Ignoring network connectivity exception", e);
                        } catch (Exception e) {
                            throw new AssertionError("unexpected", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError("unexpected", e);
                    }
                });
                relocationStartReadyBlocked.countDown();
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });
        try {
            logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
            clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB)).execute().actionGet();

            safeAwait(relocationStartReadyBlocked);
            internalCluster().restartNode(
                clusterService().state().nodes().getMasterNode().getName(),
                new InternalTestCluster.RestartCallback()
            );
        } finally {
            logger.info("--> Unblocking actions");
            blockedListeners.onResponse(null);
        }

        // Assert number of documents
        startSearchNode();
        setReplicaCount(1, indexName);
        assertFalse(clusterAdmin().prepareHealth(indexName).setWaitForActiveShards(2).get().isTimedOut());
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
    }

    public void testSearchShardRecoveryRegistersCommit() throws Exception {
        startIndexNode();
        var searchNode = startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(indexName);
        // Create some commits
        int commits = randomIntBetween(0, 3);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            refresh(indexName);
        }
        AtomicInteger registerCommitRequestsSent = new AtomicInteger();
        var searchNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, searchNode);
        searchNodeTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                registerCommitRequestsSent.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // Start a search shard
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        assertThat(registerCommitRequestsSent.get(), equalTo(1));
    }

    public void testSearchShardRecoveryRegistrationRetryOnShardNotFound() {
        var indexNode = startIndexNode();
        startSearchNode();
        final var indexName = randomIdentifier();
        var maxRetries = randomFrom(0, 5);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS))
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), maxRetries)
                .build()
        );
        ensureGreen(indexName);
        // Create some commits
        int commits = randomIntBetween(0, 3);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            refresh(indexName);
        }
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        // Make sure we hit the transport action's retries by failing more than the number of allocation attempts
        final var toFailCount = maxRetries + 1;
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger receivedRegistration = new AtomicInteger();
        var indexNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNode);
        indexNodeTransport.addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
            receivedRegistration.incrementAndGet();
            if (failed.get() < toFailCount) {
                failed.incrementAndGet();
                channel.sendResponse(new ShardNotFoundException(shardId, "can't register"));
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        // Trigger enough cluster state updates to see the reties succeed.
        for (int i = 0; i < toFailCount + 1; i++) {
            indicesAdmin().preparePutMapping(indexName).setSource("field" + i, "type=keyword").get();
        }
        ensureGreen(indexName);
        assertThat(failed.get(), equalTo(toFailCount));
        assertThat(receivedRegistration.get(), greaterThan(toFailCount));
    }
}
