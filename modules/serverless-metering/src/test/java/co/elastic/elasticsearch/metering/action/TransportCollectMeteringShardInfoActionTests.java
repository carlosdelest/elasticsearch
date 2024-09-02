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

package co.elastic.elasticsearch.metering.action;

import co.elastic.elasticsearch.metering.MeteringIndexInfoTask;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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

import static co.elastic.elasticsearch.metering.action.TestTransportActionUtils.TASK_ALLOCATION_ID;
import static co.elastic.elasticsearch.metering.action.TestTransportActionUtils.createMockClusterState;
import static co.elastic.elasticsearch.metering.action.TestTransportActionUtils.createMockClusterStateWithPersistentTask;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCollectMeteringShardInfoActionTests extends ESTestCase {
    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";
    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;

    private TransportCollectMeteringShardInfoAction action;
    private TransportService transportService;

    private class TestTransportCollectMeteringShardInfoAction extends TransportCollectMeteringShardInfoAction {
        TestTransportCollectMeteringShardInfoAction() {
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
            TransportCollectMeteringShardInfoActionTests.class.getSimpleName(),
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
        action = new TestTransportCollectMeteringShardInfoAction();
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

        var request = new CollectMeteringShardInfoAction.Request();
        PlainActionFuture<CollectMeteringShardInfoAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        final Set<DiscoveryNode> searchNodes = clusterService.state()
            .nodes()
            .stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toSet());

        // check a request was sent to the right number of nodes
        assertEquals(searchNodes.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(searchNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()), capturedRequests.keySet());

        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each node
            assertEquals(1, entry.getValue().size());

            var capturedRequest = entry.getValue().get(0);
            assertThat(capturedRequest.action(), is("cluster:monitor/get/metering/shard-info"));
            assertThat(capturedRequest.request(), instanceOf(GetMeteringShardInfoAction.Request.class));
            assertThat(
                asInstanceOf(GetMeteringShardInfoAction.Request.class, capturedRequest.request()).getCacheToken(),
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

        final Map<String, Map<ShardId, MeteringShardInfo>> nodesShardAnswer = clusterService.state()
            .nodes()
            .stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, x -> {
                var shardCount = randomIntBetween(0, shards.size());
                var nodeShards = randomSubsetOf(shardCount, shards);
                return nodeShards.stream()
                    .collect(Collectors.toMap(Function.identity(), TestTransportActionUtils::createMeteringShardInfo));
            }));

        var request = new CollectMeteringShardInfoAction.Request();
        PlainActionFuture<CollectMeteringShardInfoAction.Response> listener = new PlainActionFuture<>();

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
                    var response = nodesShardAnswer.get(nodeId);
                    seenShards.addAll(response.keySet());
                    transport.handleResponse(requestId, new GetMeteringShardInfoAction.Response(response));
                }
            }
        }

        var totalShards = seenShards.size();
        var response = listener.get();
        assertEquals("total shards", totalShards, response.getShardInfo().size());
        // response.isComplete -> totalFailed == 0;
        assertTrue("if response.isComplete no failed shards", response.isComplete() == false || totalFailed == 0);
        assertEquals(nodesShardAnswer.size(), totalSuccess + totalFailed);
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

        final Map<String, Map<ShardId, MeteringShardInfo>> nodesShardAnswer = Map.of(
            searchNodes.get(0).getId(),
            Map.ofEntries(
                Map.entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, 14L, 0L)),
                Map.entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, 15L, 0L)),
                Map.entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1, 16L, 0L))
            ),
            searchNodes.get(1).getId(),
            Map.ofEntries(
                Map.entry(shard1Id, new MeteringShardInfo(21L, 210L, 2, 1, 24L, 0L)),
                Map.entry(shard2Id, new MeteringShardInfo(22L, 220L, 1, 1, 25L, 0L))
            )
        );

        var expectedSizes = Map.of(shard1Id, 21L, shard2Id, 12L, shard3Id, 13L);
        var expectedIngestedSizes = Map.of(shard1Id, 24L, shard2Id, 15L, shard3Id, 16L);

        var request = new CollectMeteringShardInfoAction.Request();
        PlainActionFuture<CollectMeteringShardInfoAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            var nodeId = entry.getKey();
            for (var capturedRequest : entry.getValue()) {
                long requestId = capturedRequest.requestId();
                var response = nodesShardAnswer.get(nodeId);
                transport.handleResponse(requestId, new GetMeteringShardInfoAction.Response(response));
            }
        }

        var response = listener.get();
        assertEquals("total shards", 3, response.getShardInfo().size());
        assertTrue(response.isComplete());
        assertThat(response.getShardInfo().get(shard1Id).sizeInBytes(), is(expectedSizes.get(shard1Id)));
        assertThat(response.getShardInfo().get(shard2Id).sizeInBytes(), is(expectedSizes.get(shard2Id)));
        assertThat(response.getShardInfo().get(shard3Id).sizeInBytes(), is(expectedSizes.get(shard3Id)));

        assertThat(response.getShardInfo().get(shard1Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard1Id)));
        assertThat(response.getShardInfo().get(shard2Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard2Id)));
        assertThat(response.getShardInfo().get(shard3Id).storedIngestSizeInBytes(), is(expectedIngestedSizes.get(shard3Id)));
    }

    public void testNoPersistentTaskNodeFails() {
        createMockClusterState(clusterService);

        var request = new CollectMeteringShardInfoAction.Request();
        PlainActionFuture<CollectMeteringShardInfoAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);

        var exception = expectThrows(ExecutionException.class, listener::get);
        assertThat(exception.getCause(), instanceOf(PersistentTaskNodeNotAssignedException.class));
    }

    public void testWrongPersistentTaskNodeFails() {
        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(true);
        when(task.getExecutorNode()).thenReturn("FOO");

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(MeteringIndexInfoTask.TASK_NAME, task));

        createMockClusterState(clusterService, 3, 2, b -> {
            b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build());
        });

        var request = new CollectMeteringShardInfoAction.Request();
        PlainActionFuture<CollectMeteringShardInfoAction.Response> listener = new PlainActionFuture<>();

        action.doExecute(mock(Task.class), request, listener);
        flushThreadPoolExecutor(THREAD_POOL, TEST_THREAD_POOL_NAME);

        var exception = expectThrows(ExecutionException.class, listener::get);
        assertThat(exception.getCause(), instanceOf(NotPersistentTaskNodeException.class));
    }
}
