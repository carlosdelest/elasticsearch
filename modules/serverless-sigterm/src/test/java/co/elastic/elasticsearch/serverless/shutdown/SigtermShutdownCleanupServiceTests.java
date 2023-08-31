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

package co.elastic.elasticsearch.serverless.shutdown;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static co.elastic.elasticsearch.serverless.shutdown.SigtermShutdownCleanupService.CleanupSigtermShutdownTask;
import static co.elastic.elasticsearch.serverless.shutdown.SigtermShutdownCleanupService.RemoveSigtermShutdownTaskExecutor;
import static co.elastic.elasticsearch.serverless.shutdown.SigtermShutdownCleanupService.SubmitCleanupSigtermShutdown;
import static co.elastic.elasticsearch.serverless.shutdown.SigtermShutdownCleanupService.computeDelay;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.theInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class SigtermShutdownCleanupServiceTests extends ESTestCase {

    private static final long GRACE_PERIOD = 60_000;

    public void testCleanIfRemoved() {
        var mocks = newMocks();
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);
        var master = DiscoveryNodeUtils.create("node1", randomNodeId());
        var other = DiscoveryNodeUtils.create("node2", randomNodeId());
        var another = DiscoveryNodeUtils.create("node3", randomNodeId());

        sigtermShutdownService.clusterChanged(
            new ClusterChangedEvent(
                this.getTestName(),
                createClusterState(
                    new NodesShutdownMetadata(Map.of(another.getId(), sigtermShutdown(another.getId(), GRACE_PERIOD, 0))),
                    master,
                    other
                ),
                createClusterState(null, master, other, another)
            )
        );

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));

        var schedule = verifySchedule(mocks.threadPool, 1);
        schedule.shutdown.get(0).run();

        // Callable removes itself
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(0));

        // Callable submits task
        Mockito.verify(taskQueue, times(1))
            .submitTask(eq("sigterm-grace-period-expired"), any(SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class), isNull());
    }

    public void testDontCleanIfPresent() {
        var mocks = newMocks();
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        var master = DiscoveryNodeUtils.create("node1", randomNodeId());
        var other = DiscoveryNodeUtils.create("node2", randomNodeId());
        var another = DiscoveryNodeUtils.create("node3", randomNodeId());

        var shutdowns = new NodesShutdownMetadata(
            Map.of(
                another.getId(),
                sigtermShutdown(another.getId(), GRACE_PERIOD, 0),
                other.getId(),
                sigtermShutdown(other.getId(), GRACE_PERIOD, 20L)
            )
        );

        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other, another),
            createClusterState(shutdowns, master, other, another)
        );

        newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);
        sigtermShutdownService.clusterChanged(clusterChanged);

        var schedule = verifySchedule(mocks.threadPool, 2);

        assertThat(
            RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(schedule.nodeDelay.keySet(), clusterChanged.state()),
            sameInstance(clusterChanged.state())
        );
    }

    @SuppressWarnings("unchecked")
    public void testExecutorRemoveStale() {
        var mocks = newMocks();
        long now = 100_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);

        long grace = GRACE_PERIOD;
        var master = DiscoveryNodeUtils.create("master1", randomNodeId());
        var notTermNode = DiscoveryNodeUtils.create("notTerm2", randomNodeId()); // not term
        var stillExistingNode = DiscoveryNodeUtils.create("stillExisting3", randomNodeId()); // exists
        var withinGraceNode = DiscoveryNodeUtils.create("withinGrace4", randomNodeId()); // within grace
        var outOfGrace2XNode = DiscoveryNodeUtils.create("outOfGrace2X5", randomNodeId()); // out of grace
        var justOutOfGraceNode = DiscoveryNodeUtils.create("justOutOfGrace6", randomNodeId()); // also out of grace

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.masterNodeId(master.getId());
        nodesBuilder.localNodeId(master.getId());
        nodesBuilder.add(master);
        nodesBuilder.add(stillExistingNode);
        Metadata.Builder metadataBuilder = Metadata.builder();
        var shutdownsMap = Map.of(
            notTermNode.getId(),
            otherShutdown(notTermNode.getId(), SingleNodeShutdownMetadata.Type.REPLACE, now - 2 * grace).setTargetNodeName(
                notTermNode.getId()
            ).build(),
            stillExistingNode.getId(),
            sigtermShutdown(stillExistingNode.getId(), grace, now - 2 * grace),
            withinGraceNode.getId(),
            sigtermShutdown(withinGraceNode.getId(), grace, now - grace),
            outOfGrace2XNode.getId(),
            sigtermShutdown(outOfGrace2XNode.getId(), grace, now - 2 * grace),
            justOutOfGraceNode.getId(),
            sigtermShutdown(justOutOfGraceNode.getId(), grace, now - (grace + grace / 10) - 1)
        );
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownsMap));
        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();

        sigtermShutdownService.clusterChanged(new ClusterChangedEvent(this.getTestName(), state, state));

        var schedule = verifySchedule(mocks.threadPool, 4);
        var remove = schedule.collect((s, d) -> {
            if (TimeValue.ZERO.equals(d)) {
                s.remove().accept(s.nodeId());
                return true;
            }
            return false;
        });

        var update = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(remove, state);

        assertThat(
            Map.of(
                notTermNode.getId(),
                shutdownsMap.get(notTermNode.getId()),
                stillExistingNode.getId(),
                shutdownsMap.get(stillExistingNode.getId()),
                withinGraceNode.getId(),
                shutdownsMap.get(withinGraceNode.getId())
            ),
            equalTo(update.getMetadata().nodeShutdowns().getAll())
        );

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(withinGraceNode.getId()));
    }

    @SuppressWarnings("unchecked")
    public void testExecutorInitialStateIfFresh() {
        var mocks = newMocks();
        long now = 100_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        var master = DiscoveryNodeUtils.create("master1", randomNodeId());
        var other = DiscoveryNodeUtils.create("other2", randomNodeId()); // not term
        var another = DiscoveryNodeUtils.create("another3", randomNodeId()); // exists
        var yetAnother = DiscoveryNodeUtils.create("yetAnother4", randomNodeId()); // within grace

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.masterNodeId(master.getId());
        nodesBuilder.localNodeId(master.getId());
        nodesBuilder.add(master);
        nodesBuilder.add(another);
        Metadata.Builder metadataBuilder = Metadata.builder();
        var shutdownsMap = Map.of(
            other.getId(),
            otherShutdown(other.getId(), SingleNodeShutdownMetadata.Type.REPLACE, now - 2 * grace).setTargetNodeName(other.getId()).build(),
            another.getId(),
            sigtermShutdown(another.getId(), grace, now - 2 * grace),
            yetAnother.getId(),
            sigtermShutdown(yetAnother.getId(), grace, now - grace)
        );
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownsMap));
        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);
        sigtermShutdownService.clusterChanged(new ClusterChangedEvent(this.getTestName(), state, state));

        var schedule = verifySchedule(mocks.threadPool, 2);
        var remove = schedule.collect((s, d) -> {
            if (TimeValue.ZERO.equals(d)) {
                s.remove().accept(s.nodeId());
                return true;
            }
            return false;
        });

        var update = SigtermShutdownCleanupService.RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(remove, state);

        assertThat(state, sameInstance(update));
    }

    public void testCancelledTasksRescheduled() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = DiscoveryNodeUtils.create("node1", randomNodeId());
        var other = DiscoveryNodeUtils.create("node2", randomNodeId());
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other.getId(), grace, now - 10 * grace)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);
        sigtermShutdownService.clusterChanged(clusterChanged);

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(other.getId()));
        var cancelled = new MockScheduleCancellable(true);
        sigtermShutdownService.cleanups.put(other.getId(), cancelled);

        sigtermShutdownService.clusterChanged(clusterChanged);
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(other.getId()));
        assertThat(sigtermShutdownService.cleanups.get(other.getId()), not(sameInstance(cancelled)));

        var notCancelled = new MockScheduleCancellable(false);
        sigtermShutdownService.cleanups.put(other.getId(), notCancelled);
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(other.getId()));
        assertThat(sigtermShutdownService.cleanups.get(other.getId()), sameInstance(notCancelled));
    }

    public void testShutdownAlreadyRemoved() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = DiscoveryNodeUtils.create("node1", randomNodeId());
        var other = DiscoveryNodeUtils.create("node2", randomNodeId());
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other.getId(), grace, now - 10 * grace)));
        var removedShutdown = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(
            Set.of(other.getId()),
            createClusterState(shutdowns, master)
        );
        assertThat(removedShutdown.metadata().nodeShutdowns().getAll().values(), hasSize(0));

        var shutdownAlreadyRemoved = createClusterState(new NodesShutdownMetadata(Collections.emptyMap()), master);
        assertThat(
            RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(Set.of(other.getId()), shutdownAlreadyRemoved),
            sameInstance(shutdownAlreadyRemoved)
        );
    }

    public void testNonSigtermShutdown() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = DiscoveryNodeUtils.create("node1", randomNodeId());
        var other = DiscoveryNodeUtils.create("node2", randomNodeId());
        var shutdowns = new NodesShutdownMetadata(
            Map.of(other.getId(), otherShutdown(other.getId(), SingleNodeShutdownMetadata.Type.REMOVE, now - 10 * grace).build())
        );
        var removedShutdown = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(
            Set.of(other.getId()),
            createClusterState(shutdowns, master)
        );
        assertThat(removedShutdown.metadata().nodeShutdowns().getAll().values(), hasSize(1));
        assertThat(removedShutdown.metadata().nodeShutdowns(), equalTo(shutdowns));
    }

    public void testDifferentGracePeriods() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        var node1 = DiscoveryNodeUtils.create("node1", randomNodeId());
        long node1Grace = 93 * 60 * 1_000;
        long node1Started = 33 * 1_000;
        var node2 = DiscoveryNodeUtils.create("node2", randomNodeId());
        long node2Grace = 45 * 60 * 1_000;
        long node2Started = 6 * 60 * 1_000;
        var node3 = DiscoveryNodeUtils.create("node3", randomNodeId());
        long node3Grace = 2 * 60 * 60 * 1_000;
        long node3Started = node3Grace + 5 * 60 * 1_000;
        var shutdowns = new NodesShutdownMetadata(
            Map.of(
                node1.getId(),
                sigtermShutdown(node1.getId(), node1Grace, node1Started),
                node2.getId(),
                sigtermShutdown(node2.getId(), node2Grace, node2Started),
                node3.getId(),
                sigtermShutdown(node3.getId(), node3Grace, node3Started)
            )
        );
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, node1, node2, node3),
            createClusterState(shutdowns, node1, node2, node3)
        );

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService);
        long now = 10_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        sigtermShutdownService.clusterChanged(clusterChanged);

        var schedule = verifySchedule(mocks.threadPool, 3);
        assertThat(schedule.nodeDelay.get(node1.getId()), equalTo(computeDelay(now, node1Started, node1Grace)));
        assertThat(schedule.nodeDelay.get(node2.getId()), equalTo(computeDelay(now, node2Started, node2Grace)));
        assertThat(schedule.nodeDelay.get(node3.getId()), equalTo(computeDelay(now, node3Started, node3Grace)));
    }

    public void testComputeDelay() {
        long now = 100_000;
        long grace = 1_000;
        long grace_safety = grace + (grace / 10);

        // started in the future, elapsed = 0
        assertThat(computeDelay(now, now + grace, grace), equalTo(new TimeValue(grace_safety)));

        long running = 123;
        assertThat(computeDelay(now, now - running, grace), equalTo(new TimeValue(grace_safety - running)));

        assertThat(computeDelay(now, now - 2 * grace_safety, grace), equalTo(TimeValue.ZERO));
    }

    public void testBatchesSuccess() {
        var master = DiscoveryNodeUtils.create("master1", randomNodeId());
        var nodeAExists = DiscoveryNodeUtils.create("nodeAExists", randomNodeId());
        var nodeBGone = DiscoveryNodeUtils.create("nodeBGone", randomNodeId());

        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                nodeAExists.getId(),
                                sigtermShutdown(nodeAExists.getId(), 1_000, 200),
                                nodeBGone.getId(),
                                sigtermShutdown(nodeBGone.getId(), 1_000, 200)
                            )
                        )
                    )
                    .build()
            )
            .nodes(DiscoveryNodes.builder().masterNodeId(master.getId()).localNodeId(master.getId()).add(master).add(nodeAExists))
            .build();

        var contextA = new TestCleanContext(new AtomicBoolean(false), new CleanupSigtermShutdownTask(nodeAExists.getId()));
        var contextB = new TestCleanContext(new AtomicBoolean(false), new CleanupSigtermShutdownTask(nodeBGone.getId()));
        ClusterState newState = null;
        try {
            newState = new RemoveSigtermShutdownTaskExecutor().execute(
                new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(contextA, contextB), () -> null)
            );
        } catch (Exception e) {
            fail(e.toString());
        }
        assertThat(newState, not(theInstance(state)));
        assertNotNull(newState.metadata().nodeShutdowns().get(nodeAExists.getId()));
        assertNull(newState.metadata().nodeShutdowns().get(nodeBGone.getId()));
        assertTrue(contextA.successCalled.get());
        assertTrue(contextB.successCalled.get());
    }

    private record TestCleanContext(AtomicBoolean successCalled, CleanupSigtermShutdownTask task)
        implements
            ClusterStateTaskExecutor.TaskContext<CleanupSigtermShutdownTask> {
        @Override
        public CleanupSigtermShutdownTask getTask() {
            return task;
        }

        @Override
        public void success(Runnable onPublicationSuccess) {
            onPublicationSuccess.run();
            successCalled.set(true);
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer) {}

        @Override
        public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void onFailure(Exception failure) {}

        @Override
        public Releasable captureResponseHeaders() {
            return null;
        }
    }

    static SingleNodeShutdownMetadata.Builder otherShutdown(String id, SingleNodeShutdownMetadata.Type type, long startedAt) {
        return SingleNodeShutdownMetadata.builder().setNodeId(id).setType(type).setReason("test " + id).setStartedAtMillis(startedAt);
    }

    private static SingleNodeShutdownMetadata sigtermShutdown(String name, long grace, long startedAt) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(name)
            .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
            .setGracePeriod(new TimeValue(grace))
            .setReason("test")
            .setStartedAtMillis(startedAt)
            .build();
    }

    private static ClusterState createClusterState(NodesShutdownMetadata shutdown, DiscoveryNode masterNode, DiscoveryNode... nodes) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (shutdown != null) {
            metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, shutdown);
        }
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNode != null) {
            nodesBuilder.masterNodeId(masterNode.getId());
            nodesBuilder.localNodeId(masterNode.getId());
            nodesBuilder.add(masterNode);
        }
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private record Mocks(ClusterService clusterService, ThreadPool threadPool) {}

    private static Mocks newMocks() {
        final var mocks = new Mocks(mock(ClusterService.class), mock(ThreadPool.class));
        when(mocks.clusterService.threadPool()).thenReturn(mocks.threadPool);
        when(mocks.threadPool.schedule(any(SubmitCleanupSigtermShutdown.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC)))
            .thenReturn(mock(Scheduler.ScheduledCancellable.class));
        return mocks;
    }

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<CleanupSigtermShutdownTask> newMockTaskQueue(ClusterService clusterService) {
        final var masterServiceTaskQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.<CleanupSigtermShutdownTask>createTaskQueue(eq("shutdown-sigterm-cleaner"), eq(Priority.NORMAL), any()))
            .thenReturn(masterServiceTaskQueue);
        return masterServiceTaskQueue;
    }

    public record MockScheduleCancellable(boolean isCancelled) implements Scheduler.ScheduledCancellable {
        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public boolean cancel() {
            return false;
        }
    }

    private record Schedule(List<SubmitCleanupSigtermShutdown> shutdown, List<TimeValue> delay, Map<String, TimeValue> nodeDelay) {
        Set<String> collect(BiPredicate<SubmitCleanupSigtermShutdown, TimeValue> shouldCollect) {
            var set = new HashSet<String>();
            for (int i = 0; i < shutdown.size(); i++) {
                var submit = shutdown.get(i);
                if (shouldCollect.test(submit, delay.get(i))) {
                    set.add(submit.nodeId());
                }
            }
            return set;
        }
    }

    private Schedule verifySchedule(ThreadPool threadPool, int times) {
        var shutdown = ArgumentCaptor.forClass(SigtermShutdownCleanupService.SubmitCleanupSigtermShutdown.class);
        var delay = ArgumentCaptor.forClass(TimeValue.class);
        Mockito.verify(threadPool, times(times)).schedule(shutdown.capture(), delay.capture(), any(String.class));
        var schedule = new Schedule(shutdown.getAllValues(), delay.getAllValues(), new HashMap<>());
        assertThat(schedule.shutdown, hasSize(schedule.delay.size()));
        for (int i = 0; i < schedule.shutdown.size(); i++) {
            schedule.nodeDelay.put(schedule.shutdown.get(i).nodeId(), schedule.delay.get(i));
        }
        return schedule;
    }
}
