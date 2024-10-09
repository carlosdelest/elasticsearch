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

package co.elastic.elasticsearch.metering.sampling.action;

import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.activitytracking.ActivityTests;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.NotPersistentTaskNodeException;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.TASK_ALLOCATION_ID;
import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterState;
import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask;
import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCollectClusterSamplesActionTests extends ESTestCase {
    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";
    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;

    private TransportCollectClusterSamplesAction action;
    private TransportService transportService;

    private class TestTransportCollectClusterSamplesAction extends TransportCollectClusterSamplesAction {
        TestTransportCollectClusterSamplesAction() {
            super(
                transportService,
                clusterService,
                new ActionFilters(Set.of()),
                transportService.getThreadPool().executor(TEST_THREAD_POOL_NAME)
            );
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(
            TransportCollectClusterSamplesActionTests.class.getSimpleName(),
            new ScalingExecutorBuilder(TEST_THREAD_POOL_NAME, 1, 1, TimeValue.timeValueSeconds(60), true)
        );
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        action = new TestTransportCollectClusterSamplesAction();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService, transportService);
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // set static field to null to make it eligible for collection
        THREAD_POOL = null;
    }

    public void testOneRequestIsSentToEachNode() {
        createMockClusterStateWithPersistentTask(clusterService);

        var request = new CollectClusterSamplesAction.Request();
        PlainActionFuture<CollectClusterSamplesAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        DiscoveryNodes nodes = clusterService.state().nodes();

        // check a request was sent to the right number of nodes
        assertEquals(nodes.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()), capturedRequests.keySet());

        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each node
            assertEquals(1, entry.getValue().size());

            var capturedRequest = entry.getValue().get(0);
            assertThat(capturedRequest.action(), is("cluster:monitor/get/metering/samples"));
            assertThat(capturedRequest.request(), instanceOf(GetNodeSamplesAction.Request.class));
            assertThat(
                asInstanceOf(GetNodeSamplesAction.Request.class, capturedRequest.request()).getCacheToken(),
                equalTo(Long.toString(TASK_ALLOCATION_ID))
            );
        }
    }

    public void testResultAggregation() throws ExecutionException, InterruptedException {
        createMockClusterStateWithPersistentTask(clusterService);
        var shards = List.of(
            new ShardId("index1", "index1UUID", 1),
            new ShardId("index1", "index1UUID", 2),
            new ShardId("index2", "index2UUID", 1),
            new ShardId("index2", "index2UUID", 2),
            new ShardId("index2", "index2UUID", 3)
        );

        var nodes = clusterService.state().nodes();
        var nodeIds = nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        var successNodeIds = rarely() ? randomSubsetOf(nodeIds) : nodeIds;

        var nodesShardAnswers = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, x -> {
                var shardCount = randomIntBetween(0, shards.size());
                var nodeShards = randomSubsetOf(shardCount, shards);
                return nodeShards.stream()
                    .collect(Collectors.toMap(identity(), TransportCollectClusterSamplesActionTests::createMeteringShardInfo));
            }));

        var searchNodeMemory = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, n -> randomLongBetween(0, 10000)));
        var indexNodeMemory = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, n -> randomLongBetween(0, 10000)));

        var nodeSearchActivity = nodeIds.stream().collect(Collectors.toMap(identity(), (k) -> ActivityTests.randomActivity()));
        var nodeIndexActivity = nodeIds.stream().collect(Collectors.toMap(identity(), (k) -> ActivityTests.randomActivity()));

        var request = new CollectClusterSamplesAction.Request();
        var listener = new PlainActionFuture<CollectClusterSamplesAction.Response>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        int totalSuccess = 0;
        Set<ShardId> seenShards = new HashSet<>();
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            var nodeId = entry.getKey();
            for (var capturedRequest : entry.getValue()) {
                long requestId = capturedRequest.requestId();
                if (successNodeIds.contains(nodeId) == false) {
                    // simulate node failure
                    transport.handleRemoteError(requestId, new Exception());
                    continue;
                }
                totalSuccess++;
                var shardInfos = nodesShardAnswers.getOrDefault(nodeId, emptyMap());
                seenShards.addAll(shardInfos.keySet());
                var memory = searchNodeMemory.containsKey(nodeId) ? searchNodeMemory.get(nodeId) : indexNodeMemory.getOrDefault(nodeId, 0L);
                var searchActivity = nodeSearchActivity.get(nodeId);
                var indexActivity = nodeIndexActivity.get(nodeId);
                transport.handleResponse(requestId, new GetNodeSamplesAction.Response(memory, searchActivity, indexActivity, shardInfos));
            }
        }

        var totalShards = seenShards.size();
        var response = listener.get();

        assertThat(response.isComplete(), is(nodeIds.size() == successNodeIds.size()));
        assertThat(totalSuccess, is(successNodeIds.size()));
        assertThat(response.getFailures(), hasSize(nodes.size() - successNodeIds.size()));

        assertThat(response.getShardInfos(), aMapWithSize(totalShards));

        // only retain answers of successful nodes
        searchNodeMemory.keySet().retainAll(successNodeIds);
        indexNodeMemory.keySet().retainAll(successNodeIds);
        nodeSearchActivity.keySet().retainAll(successNodeIds);
        nodeIndexActivity.keySet().retainAll(successNodeIds);

        assertEquals(
            "search tier memory",
            searchNodeMemory.values().stream().mapToLong(Long::longValue).sum(),
            response.getSearchTierMemorySize()
        );
        assertEquals(
            "index tier memory",
            indexNodeMemory.values().stream().mapToLong(Long::longValue).sum(),
            response.getIndexTierMemorySize()
        );

        var coolDown = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
        assertEquals("search activity", Activity.merge(nodeSearchActivity.values().stream(), coolDown), response.getSearchActivity());
        assertEquals("index activity", Activity.merge(nodeIndexActivity.values().stream(), coolDown), response.getIndexActivity());
    }

    public void testMostRecentUsedInResultAggregation() throws ExecutionException, InterruptedException {
        createMockClusterStateWithPersistentTask(clusterService);

        var searchNodes = clusterService.state()
            .nodes()
            .stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .toList();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        // only search nodes will answer with shards, index nodes will return an empty map
        final Map<String, Map<ShardId, ShardInfoMetrics>> nodesShardAnswer = Map.of(
            searchNodes.get(0).getId(),
            Map.ofEntries(
                Map.entry(shard1Id, new ShardInfoMetrics(110L, 11L, 0L, 14L, 1, 1, 0L)),
                Map.entry(shard2Id, new ShardInfoMetrics(120L, 12L, 0L, 15L, 1, 2, 0L)),
                Map.entry(shard3Id, new ShardInfoMetrics(130L, 13L, 0L, 16L, 1, 1, 0L))
            ),
            searchNodes.get(1).getId(),
            Map.ofEntries(
                Map.entry(shard1Id, new ShardInfoMetrics(210L, 21L, 0L, 24L, 2, 1, 0L)),
                Map.entry(shard2Id, new ShardInfoMetrics(220L, 22L, 0L, 25L, 1, 1, 0L))
            )
        );

        var expectedSizes = Map.of(shard1Id, 21L, shard2Id, 12L, shard3Id, 13L);
        var expectedIngestedSizes = Map.of(shard1Id, 24L, shard2Id, 15L, shard3Id, 16L);

        var request = new CollectClusterSamplesAction.Request();
        PlainActionFuture<CollectClusterSamplesAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            var nodeId = entry.getKey();
            for (var capturedRequest : entry.getValue()) {
                long requestId = capturedRequest.requestId();
                var response = nodesShardAnswer.getOrDefault(nodeId, emptyMap());
                transport.handleResponse(requestId, new GetNodeSamplesAction.Response(0, Activity.EMPTY, Activity.EMPTY, response));
            }
        }

        var response = listener.get();
        assertEquals("total shards", 3, response.getShardInfos().size());
        assertTrue(response.isComplete());
        assertThat(response.getShardInfos().get(shard1Id).totalSizeInBytes(), is(expectedSizes.get(shard1Id)));
        assertThat(response.getShardInfos().get(shard2Id).totalSizeInBytes(), is(expectedSizes.get(shard2Id)));
        assertThat(response.getShardInfos().get(shard3Id).totalSizeInBytes(), is(expectedSizes.get(shard3Id)));

        assertThat(response.getShardInfos().get(shard1Id).rawStoredSizeInBytes(), is(expectedIngestedSizes.get(shard1Id)));
        assertThat(response.getShardInfos().get(shard2Id).rawStoredSizeInBytes(), is(expectedIngestedSizes.get(shard2Id)));
        assertThat(response.getShardInfos().get(shard3Id).rawStoredSizeInBytes(), is(expectedIngestedSizes.get(shard3Id)));
    }

    public void testNoPersistentTaskNodeFails() {
        createMockClusterState(clusterService);

        var request = new CollectClusterSamplesAction.Request();
        PlainActionFuture<CollectClusterSamplesAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);

        var exception = expectThrows(ExecutionException.class, listener::get);
        assertThat(exception.getCause(), instanceOf(PersistentTaskNodeNotAssignedException.class));
    }

    public void testWrongPersistentTaskNodeFails() {
        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(true);
        when(task.getExecutorNode()).thenReturn("FOO");

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(clusterService, 3, 2, b -> {
            b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build());
        });

        var request = new CollectClusterSamplesAction.Request();
        PlainActionFuture<CollectClusterSamplesAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);

        var exception = expectThrows(ExecutionException.class, listener::get);
        assertThat(exception.getCause(), instanceOf(NotPersistentTaskNodeException.class));
    }

    static ShardInfoMetrics createMeteringShardInfo(ShardId shardId) {
        var size = ESTestCase.randomLongBetween(0, 10000);
        return new ShardInfoMetrics(ESTestCase.randomLongBetween(0, 10000), size, 0L, size, 0, 0, 0L);
    }
}
