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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.TASK_ALLOCATION_ID;
import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterState;
import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
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
            assertThat(capturedRequest.action(), is("cluster:monitor/get/metering/shard-info"));
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

        final DiscoveryNodes nodes = clusterService.state().nodes();
        final Map<String, Map<ShardId, ShardInfoMetrics>> nodesShardAnswers = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, x -> {
                var shardCount = randomIntBetween(0, shards.size());
                var nodeShards = randomSubsetOf(shardCount, shards);
                return nodeShards.stream()
                    .collect(Collectors.toMap(Function.identity(), TransportCollectClusterSamplesActionTests::createMeteringShardInfo));
            }));

        final Map<String, Long> searchNodeMemoryAnswers = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, n -> randomLongBetween(0, 10000)));

        final Map<String, Long> indexNodeMemoryAnswers = nodes.stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, n -> randomLongBetween(0, 10000)));

        var request = new CollectClusterSamplesAction.Request();
        PlainActionFuture<CollectClusterSamplesAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        Set<ShardId> seenShards = new HashSet<>();
        int totalSuccess = 0;
        int totalFailed = 0;
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            var nodeId = entry.getKey();
            for (var capturedRequest : entry.getValue()) {
                long requestId = capturedRequest.requestId();
                if (rarely()) {
                    // simulate node failure
                    totalFailed++;
                    transport.handleRemoteError(requestId, new Exception());
                } else {
                    totalSuccess++;
                    var shardInfos = nodesShardAnswers.getOrDefault(nodeId, Collections.emptyMap());
                    seenShards.addAll(shardInfos.keySet());
                    var memory = searchNodeMemoryAnswers.containsKey(nodeId)
                        ? searchNodeMemoryAnswers.get(nodeId)
                        : indexNodeMemoryAnswers.getOrDefault(nodeId, 0L);
                    transport.handleResponse(requestId, new GetNodeSamplesAction.Response(memory, shardInfos));
                }
            }
        }

        var totalShards = seenShards.size();
        var response = listener.get();
        assertEquals("total shards", totalShards, response.getShardInfos().size());
        assertEquals(
            "search tier memory",
            searchNodeMemoryAnswers.values().stream().mapToLong(Long::longValue).sum(),
            response.getSearchTierMemorySize()
        );
        assertEquals(
            "index tier memory",
            indexNodeMemoryAnswers.values().stream().mapToLong(Long::longValue).sum(),
            response.getIndexTierMemorySize()
        );
        // response.isComplete -> totalFailed == 0;
        assertTrue("if response.isComplete no failed shards", response.isComplete() == false || totalFailed == 0);
        assertEquals(nodes.size(), totalSuccess + totalFailed);
        assertEquals("accumulated exceptions", totalFailed, response.getFailures().size());
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
                Map.entry(shard1Id, new ShardInfoMetrics(11L, 110L, 1, 1, 14L, 0L)),
                Map.entry(shard2Id, new ShardInfoMetrics(12L, 120L, 1, 2, 15L, 0L)),
                Map.entry(shard3Id, new ShardInfoMetrics(13L, 130L, 1, 1, 16L, 0L))
            ),
            searchNodes.get(1).getId(),
            Map.ofEntries(
                Map.entry(shard1Id, new ShardInfoMetrics(21L, 210L, 2, 1, 24L, 0L)),
                Map.entry(shard2Id, new ShardInfoMetrics(22L, 220L, 1, 1, 25L, 0L))
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
                var response = nodesShardAnswer.getOrDefault(nodeId, Collections.emptyMap());
                transport.handleResponse(requestId, new GetNodeSamplesAction.Response(0, response));
            }
        }

        var response = listener.get();
        assertEquals("total shards", 3, response.getShardInfos().size());
        assertTrue(response.isComplete());
        assertThat(response.getShardInfos().get(shard1Id).sizeInBytes(), is(expectedSizes.get(shard1Id)));
        assertThat(response.getShardInfos().get(shard2Id).sizeInBytes(), is(expectedSizes.get(shard2Id)));
        assertThat(response.getShardInfos().get(shard3Id).sizeInBytes(), is(expectedSizes.get(shard3Id)));

        assertThat(response.getShardInfos().get(shard1Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard1Id)));
        assertThat(response.getShardInfos().get(shard2Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard2Id)));
        assertThat(response.getShardInfos().get(shard3Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard3Id)));
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
        return new ShardInfoMetrics(size, ESTestCase.randomLongBetween(0, 10000), 0, 0, size, 0L);
    }
}
