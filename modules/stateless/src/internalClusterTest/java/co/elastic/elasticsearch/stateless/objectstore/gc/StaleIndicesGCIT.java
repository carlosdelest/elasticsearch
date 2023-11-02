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

package co.elastic.elasticsearch.stateless.objectstore.gc;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class StaleIndicesGCIT extends AbstractStatelessIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), BlockingDeletesRepoPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1));
    }

    public void testStaleIndicesAreCleanedEventually() throws Exception {
        var masterNode = startMasterNode();

        var indexNode = startIndexNode();
        startIndexNode();
        ensureStableCluster(3);

        var indexName = randomIdentifier();
        createAndPopulateIndex(indexName, indexNode);

        var indexUUID = resolveIndexUUID(indexName);
        internalCluster().stopNode(indexNode);

        ensureRed(masterNode);
        assertIndexExistsInObjectStore(indexUUID);

        client().admin().indices().prepareDelete(indexName).get();

        assertBusy(() -> assertIndexDoesNotExistsInObjectStore(indexUUID));
    }

    public void testStaleIndicesAreCleanedAfterAMasterFailover() throws Exception {
        var masterNode = startMasterNode();
        var masterNode2 = startMasterNode();

        var indexNode = startIndexNode();
        startIndexNode();
        ensureStableCluster(4);

        var stoppedMasterNode = internalCluster().getMasterName();

        var indexName = randomIdentifier();
        createAndPopulateIndex(indexName, indexNode);

        internalCluster().stopCurrentMasterNode();

        internalCluster().stopNode(indexNode);

        var indexUUID = resolveIndexUUID(indexName);

        ensureRed(stoppedMasterNode.equals(masterNode) ? masterNode2 : masterNode);
        assertIndexExistsInObjectStore(indexUUID);

        client().admin().indices().prepareDelete(indexName).get();

        assertBusy(() -> assertIndexDoesNotExistsInObjectStore(indexUUID));
    }

    public void testStaleIndicesAreCleanedAfterThePersistentTaskNodeFails() throws Exception {
        startMasterNode();

        // Since StaleIndicesGCTask.TASK_NAME is only allocated in Index nodes,
        // it will be allocated in this node.
        var indexNode = startIndexNode();
        ensureStableCluster(2);

        var indexName = randomIdentifier();
        createAndPopulateIndex(indexName, indexNode);

        internalCluster().stopNode(indexNode);

        var indexUUID = resolveIndexUUID(indexName);
        assertIndexExistsInObjectStore(indexUUID);

        client().admin().indices().prepareDelete(indexName).get();

        // no index node can take care of cleaning the stale files
        assertIndexExistsInObjectStore(indexUUID);

        startIndexNode();

        assertBusy(() -> assertIndexDoesNotExistsInObjectStore(indexUUID));
    }

    public void testStaleIndicesAreCleanedOnlyWhenGCIsEnabled() throws Exception {
        startMasterNode(Settings.builder().put(ObjectStoreGCTask.STALE_INDICES_GC_ENABLED_SETTING.getKey(), false).build());

        var indexNode = startIndexNode(Settings.builder().put(ObjectStoreGCTask.STALE_INDICES_GC_ENABLED_SETTING.getKey(), false).build());
        startIndexNode(Settings.builder().put(ObjectStoreGCTask.STALE_INDICES_GC_ENABLED_SETTING.getKey(), false).build());
        ensureStableCluster(3);

        var indexName = randomIdentifier();
        createAndPopulateIndex(indexName, indexNode);

        var indexUUID = resolveIndexUUID(indexName);
        internalCluster().stopNode(indexNode);

        assertIndexExistsInObjectStore(indexUUID);

        client().admin().indices().prepareDelete(indexName).get();

        safeSleep(5000);
        assertIndexExistsInObjectStore(indexUUID);
    }

    enum DisruptionScenario {
        ISOLATED_NODE_RUNNING_GC,
        BLOCKED_CLUSTER_STATE_APPLIER
    }

    public void testBlockedClusterStateApplierInNodeRunningGCDoesNotDeleteNewIndexData() throws Exception {
        doTestNoNewIndexDataIsDeletedUnderDisruptions(DisruptionScenario.BLOCKED_CLUSTER_STATE_APPLIER);
    }

    public void testIsolatedNodeRunningGCDoesNotDeleteNewIndexData() throws Exception {
        doTestNoNewIndexDataIsDeletedUnderDisruptions(DisruptionScenario.ISOLATED_NODE_RUNNING_GC);
    }

    public void doTestNoNewIndexDataIsDeletedUnderDisruptions(DisruptionScenario disruptionScenario) throws Exception {
        var masterNode = startMasterNode(
            Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(5)).build()
        );

        var indexNode = startIndexNode();
        var indexNode2 = startIndexNode();
        ensureStableCluster(3);

        var executingTaskNode = getNodeWhereGCTaskIsAssigned();
        var nodeWhereIndexIsAllocated = executingTaskNode.equals(indexNode2) ? indexNode : indexNode2;

        var executingTaskNodeObjectStore = ObjectStoreTestUtils.getObjectStoreMockRepository(
            internalCluster().getInstance(ObjectStoreService.class, executingTaskNode),
            BlockingDeletesRepository.class
        );

        var disruption = switch (disruptionScenario) {
            case ISOLATED_NODE_RUNNING_GC -> new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(executingTaskNode), Set.of(masterNode, nodeWhereIndexIsAllocated)),
                NetworkDisruption.UNRESPONSIVE
            );
            case BLOCKED_CLUSTER_STATE_APPLIER -> new BlockClusterStateProcessing(executingTaskNode, random());
        };
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        // Block the isolated node object store list to ensure that it will get the newly created index
        executingTaskNodeObjectStore.blockGetChildren();
        executingTaskNodeObjectStore.waitUntilGetChildrenIsBlocked();

        // Once the cluster is partitioned, all requests must go through the non-isolated nodes, so they can make progress
        var newIndex = randomIdentifier();
        client(masterNode).admin()
            .indices()
            .prepareCreate(newIndex)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.require._name", nodeWhereIndexIsAllocated)
                    .build()
            )
            .execute()
            .get();

        var healthResponse = client(masterNode).admin().cluster().prepareHealth(newIndex).setWaitForGreenStatus().get();
        assertFalse(healthResponse.isTimedOut());

        var indexUUID = resolveIndexUUID(newIndex, masterNode);
        assertIndexExistsInObjectStore(indexUUID, masterNode);

        var bulkRequest = client(nodeWhereIndexIsAllocated).prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest(newIndex).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        assertNoFailures(bulkRequest.get());

        var flushResponse = client(nodeWhereIndexIsAllocated).admin().indices().prepareFlush(newIndex).get();
        assertNoFailures(flushResponse);

        executingTaskNodeObjectStore.unblockGetChildren();
        // Ensure that the isolated node has enough time to go through the listed files
        // and waits for the latest cluster state instead of deleting the newly created files
        safeSleep(5000);

        assertIndexExistsInObjectStore(indexUUID, masterNode);
        disruption.stopDisrupting();
    }

    public void testIndexDeletionAndGCConcurrentDeletes() throws Exception {
        startMasterNode();
        var indexNode = startIndexNode();
        var indexNode2 = startIndexNode();
        ensureStableCluster(3);

        var executingTaskNode = getNodeWhereGCTaskIsAssigned();
        var nodeWhereIndexIsAllocated = executingTaskNode.equals(indexNode2) ? indexNode : indexNode2;

        var newIndex = randomIdentifier();
        createAndPopulateIndex(newIndex, nodeWhereIndexIsAllocated);

        ensureGreen(newIndex);

        var indexUUID = resolveIndexUUID(newIndex);
        assertIndexExistsInObjectStore(indexUUID);

        var executingTaskNodeObjectStore = ObjectStoreTestUtils.getObjectStoreMockRepository(
            internalCluster().getInstance(ObjectStoreService.class, executingTaskNode),
            BlockingDeletesRepository.class
        );

        var nodeWhereIndexIsAllocatedObjectStore = ObjectStoreTestUtils.getObjectStoreMockRepository(
            internalCluster().getInstance(ObjectStoreService.class, nodeWhereIndexIsAllocated),
            BlockingDeletesRepository.class
        );

        executingTaskNodeObjectStore.blockDeletes();
        nodeWhereIndexIsAllocatedObjectStore.blockDeletes();

        client().admin().indices().prepareDelete(newIndex).get();

        // This is a bit implementation specific, but it's the only way to ensure that
        // both nodes are waiting on the deletion.
        executingTaskNodeObjectStore.waitUntilDeleteDirectoryIsBlocked();
        nodeWhereIndexIsAllocatedObjectStore.waitUntilSegmentDeleteIsBlocked();

        // Unblock the deletes in random order
        if (randomBoolean()) {
            executingTaskNodeObjectStore.unblockDeletes();
            nodeWhereIndexIsAllocatedObjectStore.unblockDeletes();
        } else {
            nodeWhereIndexIsAllocatedObjectStore.unblockDeletes();
            executingTaskNodeObjectStore.unblockDeletes();
        }

        assertBusy(() -> assertIndexDoesNotExistsInObjectStore(indexUUID));
    }

    private static String getNodeWhereGCTaskIsAssigned() {
        var state = client().admin().cluster().prepareState().get().getState();
        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        var nodeId = persistentTasks.getTask(ObjectStoreGCTask.TASK_NAME).getAssignment().getExecutorNode();
        var executingTaskNode = state.nodes().resolveNode(nodeId).getName();
        return executingTaskNode;
    }

    private void createAndPopulateIndex(String indexName, String indexNode) {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.require._name", indexNode)
                .build()
        );
        var numberOfSegments = randomIntBetween(5, 10);
        for (int i = 0; i < numberOfSegments; i++) {
            indexDocs(indexName, 1000);
            flush(indexName);
        }
    }

    private void assertIndexExistsInObjectStore(String indexUUID) throws Exception {
        assertThat(getIndicesInBlobStore(), contains(indexUUID));
    }

    private void assertIndexExistsInObjectStore(String indexUUID, String viaNode) throws Exception {
        assertThat(getIndicesInBlobStore(viaNode), contains(indexUUID));
    }

    private void assertIndexDoesNotExistsInObjectStore(String indexUUID) throws Exception {
        assertThat(getIndicesInBlobStore(), not(contains(indexUUID)));
    }

    private static Set<String> getIndicesInBlobStore() throws IOException {
        return getIndicesInBlobStore(null);
    }

    private static Set<String> getIndicesInBlobStore(String viaNode) throws IOException {
        var objectStoreService = viaNode == null
            ? internalCluster().getCurrentMasterNodeInstance(ObjectStoreService.class)
            : internalCluster().getInstance(ObjectStoreService.class, viaNode);
        return objectStoreService.getIndicesBlobContainer().children(OperationPurpose.INDICES).keySet();
    }

    private static String resolveIndexUUID(String indexName) {
        return resolveIndexUUID(indexName, null);
    }

    private static String resolveIndexUUID(String indexName, String viaNode) {
        return client(viaNode).admin().cluster().prepareState().get().getState().metadata().index(indexName).getIndexUUID();
    }

    private String startMasterNode() {
        return startMasterNode(Settings.EMPTY);
    }

    private String startMasterNode(Settings settings) {
        // Quick fail-over
        return internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .put(settings)
                .build()
        );
    }

    private void ensureRed(String masterNode) throws Exception {
        assertBusy(() -> {
            var healthResponse = client(masterNode).admin().cluster().prepareHealth().get();
            assertFalse(healthResponse.isTimedOut());
            assertThat(healthResponse.getStatus(), is(ClusterHealthStatus.RED));
        });
    }

    public static class BlockingDeletesRepoPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                "mock",
                (metadata) -> new BlockingDeletesRepository(
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of();
        }
    }

    static class BlockingDeletesRepository extends FsRepository {

        final AtomicBoolean blockDeletes = new AtomicBoolean();
        volatile CountDownLatch blockDeleteLatch = new CountDownLatch(0);
        volatile CountDownLatch deleteDirectoryBlocked = new CountDownLatch(0);
        volatile CountDownLatch batchDeleteBlocked = new CountDownLatch(0);

        final AtomicBoolean blockGetChildren = new AtomicBoolean();
        volatile CountDownLatch blockGetChildrenLatch = new CountDownLatch(0);
        volatile CountDownLatch getChildrenBlocked = new CountDownLatch(0);

        BlockingDeletesRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new BlockingDeletesBlobStore(super.createBlobStore());
        }

        void blockDeletes() {
            if (blockDeletes.compareAndSet(false, true)) {
                blockDeleteLatch = new CountDownLatch(1);
                deleteDirectoryBlocked = new CountDownLatch(1);
                batchDeleteBlocked = new CountDownLatch(1);
            }
        }

        void blockGetChildren() {
            if (blockGetChildren.compareAndSet(false, true)) {
                blockGetChildrenLatch = new CountDownLatch(1);
                getChildrenBlocked = new CountDownLatch(1);
            }
        }

        void waitUntilDeleteDirectoryIsBlocked() {
            safeAwait(deleteDirectoryBlocked);
        }

        void waitUntilSegmentDeleteIsBlocked() {
            safeAwait(batchDeleteBlocked);
        }

        void waitUntilGetChildrenIsBlocked() {
            safeAwait(getChildrenBlocked);
        }

        void unblockDeletes() {
            var blockLatch = blockDeleteLatch;
            if (blockDeletes.compareAndSet(true, false)) {
                blockLatch.countDown();
            }
        }

        void unblockGetChildren() {
            var blockLatch = blockGetChildrenLatch;
            if (blockGetChildren.compareAndSet(true, false)) {
                blockLatch.countDown();
            }
        }

        void maybeBlockGetChildren() {
            if (blockGetChildren.get()) {
                getChildrenBlocked.countDown();
            }
            safeAwait(blockGetChildrenLatch);
        }

        void maybeBlockDeletes(CountDownLatch latch) {
            if (blockDeletes.get()) {
                latch.countDown();
            }
            safeAwait(blockDeleteLatch);
        }

        void safeAwait(CountDownLatch latch) {
            try {
                assertTrue(latch.await(60, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }

        class BlockingDeletesBlobStore implements BlobStore {
            private final BlobStore delegate;

            BlockingDeletesBlobStore(BlobStore delegate) {
                this.delegate = delegate;
            }

            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new FilterBlobContainer(delegate.blobContainer(path)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
                        maybeBlockGetChildren();
                        return super.children(purpose);
                    }

                    @Override
                    public DeleteResult delete(OperationPurpose purpose) throws IOException {
                        maybeBlockDeletes(deleteDirectoryBlocked);
                        return super.delete(purpose);
                    }
                };
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
                maybeBlockDeletes(batchDeleteBlocked);
                delegate.deleteBlobsIgnoringIfNotExists(purpose, blobNames);
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        }
    }
}
