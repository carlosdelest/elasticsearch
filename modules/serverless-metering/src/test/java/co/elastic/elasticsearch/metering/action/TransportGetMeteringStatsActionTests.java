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

import co.elastic.elasticsearch.metering.MockedClusterStateTestUtils;
import co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledShardInfos;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;
import co.elastic.elasticsearch.metering.sampling.action.TransportCollectClusterSamplesActionTests;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterState;
import static co.elastic.elasticsearch.metering.MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskExecutor.MINIMUM_METERING_INFO_UPDATE_PERIOD;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetMeteringStatsActionTests extends ESTestCase {

    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";
    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;

    private TransportService transportService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private SampledClusterMetricsService clusterMetricsService;

    private class TestTransportGetMeteringStatsAction extends TransportGetMeteringStatsAction {
        TestTransportGetMeteringStatsAction(TimeValue meteringShardInfoUpdatePeriod, boolean projectUsesRawStorageMetric) {
            super(
                GetMeteringStatsAction.FOR_SECONDARY_USER_NAME,
                transportService,
                new ActionFilters(Set.of()),
                clusterService,
                indexNameExpressionResolver,
                clusterMetricsService,
                transportService.getThreadPool().executor(TEST_THREAD_POOL_NAME),
                meteringShardInfoUpdatePeriod,
                projectUsesRawStorageMetric
            );
        }
    }

    private static class TestGetMeteringStatsActionResponseListener implements ActionListener<GetMeteringStatsAction.Response> {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        Exception exception;
        GetMeteringStatsAction.Response response;

        @Override
        public void onResponse(GetMeteringStatsAction.Response response) {
            this.response = response;
            completionLatch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            exception = e;
            completionLatch.countDown();
        }

        void await() throws InterruptedException, TimeoutException {
            if (completionLatch.await(100, TimeUnit.SECONDS) == false) {
                throw new TimeoutException();
            }
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
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        clusterMetricsService = mock(SampledClusterMetricsService.class);
        clusterService = createClusterService(THREAD_POOL);
    }

    private TestTransportGetMeteringStatsAction createActionAndInitTransport(
        CapturingTransport transport,
        TimeValue meteringShardInfoUpdatePeriod,
        boolean projectUsesRawStorageMetric
    ) {
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
        return new TestTransportGetMeteringStatsAction(meteringShardInfoUpdatePeriod, projectUsesRawStorageMetric);
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

    public void testNoPersistentTaskNodeRetries() throws InterruptedException, TimeoutException {
        var action = createActionAndInitTransport(new CapturingTransport(), TimeValue.timeValueSeconds(20), true);

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(false, true);
        when(task.getExecutorNode()).thenReturn(MockedClusterStateTestUtils.LOCAL_NODE_ID);

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build())
        );

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        assertThat(listener.response, notNullValue());
    }

    public void testNoPersistentTaskNodeEventuallyFails() throws InterruptedException, TimeoutException {
        var action = createActionAndInitTransport(new CapturingTransport(), TimeValue.timeValueSeconds(5), true);
        createMockClusterState(clusterService);

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        assertThat(listener.exception, instanceOf(PersistentTaskNodeNotAssignedException.class));
        assertThat(
            listener.exception.getMessage(),
            containsString("PersistentTask [metering-index-info] has not been yet assigned to a node on this cluster")
        );
    }

    public void testRequestExecutedOnPersistentTaskNode() throws InterruptedException, TimeoutException {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        createMockClusterStateWithPersistentTask(clusterService);

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        assertThat(capturedRequests, anEmptyMap());
        assertThat(listener.response, notNullValue());
    }

    public void testRequestRoutedToPersistentTaskNode() throws InterruptedException, TimeoutException {
        var transport = createMockCapturingTransport(new GetMeteringStatsAction.Response(10L, 100L, Map.of(), Map.of(), Map.of()));
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        setState(clusterService, createMockClusterStateWithPersistentTask("node_1"));

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        assertThat(capturedRequests, aMapWithSize(1));

        var requestsToPersistentNode = capturedRequests.get("node_1");
        var getMeteringRequests = requestsToPersistentNode.stream()
            .filter(x -> x.action().startsWith(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME))
            .toList();
        assertThat(getMeteringRequests, hasSize(1));
    }

    public void testPersistentTaskNodeChangeDuringRequest() throws InterruptedException, TimeoutException {

        var node1Response = new GetMeteringStatsAction.Response(10L, 100L, Map.of(), Map.of(), Map.of());
        var node2Response = new GetMeteringStatsAction.Response(20L, 200L, Map.of(), Map.of(), Map.of());
        var transport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                if (node.getId().equals("node_1")) {
                    handleResponse(requestId, node1Response);
                } else if (node.getId().equals("node_2")) {
                    handleResponse(requestId, node2Response);
                } else {
                    handleError(requestId, new TransportException("invalid node"));
                }
            }
        };
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(20), true);

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(true);
        // Simulate a change in PersistentTask node allocation
        when(task.getExecutorNode()).thenReturn("node_1", "node_2");

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build())
        );

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        assertThat(capturedRequests, aMapWithSize(2));

        var requestsToOldPersistentNode = capturedRequests.get("node_1");
        assertThat(requestsToOldPersistentNode, hasSize(1));
        assertThat(requestsToOldPersistentNode.get(0).action(), is(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME));

        var requestsToNewPersistentNode = capturedRequests.get("node_2");
        assertThat(requestsToNewPersistentNode, hasSize(1));
        assertThat(requestsToNewPersistentNode.get(0).action(), is(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME));

        assertThat(listener.response, is(node2Response));
        assertThat(listener.exception, nullValue());
    }

    public void testPersistentTaskReassignedDuringRequest() throws InterruptedException, TimeoutException {

        var node1Response = new GetMeteringStatsAction.Response(10L, 100L, Map.of(), Map.of(), Map.of());
        var node2Response = new GetMeteringStatsAction.Response(20L, 200L, Map.of(), Map.of(), Map.of());

        var transport = createMockCapturingTransport(node1Response, node2Response);
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(20), true);

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        // Simulate assignment/unassignment/reassignment of PersistentTask node
        when(task.isAssigned()).thenReturn(true, false, true);
        when(task.getExecutorNode()).thenReturn("node_1");

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build())
        );

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        assertThat(capturedRequests, aMapWithSize(1));

        var requestsToPersistentNode = capturedRequests.get("node_1");
        assertThat(requestsToPersistentNode, hasSize(2));
        assertThat(requestsToPersistentNode.get(0).action(), is(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME));
        assertThat(requestsToPersistentNode.get(1).action(), is(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME));

        assertThat(listener.response, is(node2Response));
        assertThat(listener.exception, nullValue());
    }

    public void testPersistentTaskUnassignedDuringRequest() throws InterruptedException, TimeoutException {

        var node1Response = new GetMeteringStatsAction.Response(10L, 100L, Map.of(), Map.of(), Map.of());
        var transport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                handleResponse(requestId, node1Response);
            }
        };
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        // Simulate assignment/unassignment of PersistentTask node
        when(task.isAssigned()).thenReturn(true, false);
        when(task.getExecutorNode()).thenReturn("node_1");

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build())
        );

        var request = new GetMeteringStatsAction.Request();
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        assertThat(capturedRequests, aMapWithSize(1));

        var requestsToPersistentNode = capturedRequests.get("node_1");
        assertThat(requestsToPersistentNode, hasSize(1));
        assertThat(requestsToPersistentNode.get(0).action(), is(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME));

        assertThat(listener.response, nullValue());
        assertThat(listener.exception, instanceOf(PersistentTaskNodeNotAssignedException.class));
    }

    public void testCreateResponseNoShards() {
        SampledShardInfos mockShardsInfo = mock(invocation -> ShardInfoMetrics.EMPTY);

        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        createMockClusterState(clusterService, 3, 2, b -> {});

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[0]);

        assertThat(response.totalDocCount, is(0L));
        assertThat(response.totalSizeInBytes, is(0L));
    }

    public void testCreateResponseTwoIndices() {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), false);

        var index1 = new Index("index1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("index2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 10L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 20L).build()
        );

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .build()
            )
        );

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[] { "index1", "index2" });

        assertThat(response.totalDocCount, is(300L));
        assertThat(response.totalSizeInBytes, is(30L));
        assertThat(response.indexToStatsMap, aMapWithSize(2));
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.indexToStatsMap.get(index2.getName()).docCount(), is(200L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(10L));
        assertThat(response.indexToStatsMap.get(index2.getName()).sizeInBytes(), is(20L));
    }

    public void testCreateResponseTwoIndicesRawStorageProject() {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        var index1 = new Index("index1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("index2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 11L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 22L).build()
        );

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .build()
            )
        );

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[] { "index1", "index2" });

        assertThat(response.totalDocCount, is(300L));
        assertThat(response.totalSizeInBytes, is(33L));
        assertThat(response.indexToStatsMap, aMapWithSize(2));
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.indexToStatsMap.get(index2.getName()).docCount(), is(200L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(11L));
        assertThat(response.indexToStatsMap.get(index2.getName()).sizeInBytes(), is(22L));
    }

    public void testCreateResponseTwoIndicesOneDatastream() {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        var index1 = new Index("index1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("index2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 11L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 22L).build()
        );

        createMockClusterState(clusterService, 3, 2, b -> {
            b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .build()
            );
            b.metadata(
                Metadata.builder()
                    .putCustom(
                        DataStreamMetadata.TYPE,
                        new DataStreamMetadata(
                            ImmutableOpenMap.<String, DataStream>builder()
                                .fPut("dataStream1", DataStreamTestHelper.newInstance("dataStream1", List.of(index2)))
                                .build(),
                            ImmutableOpenMap.of()
                        )
                    )
                    .indices(createLocalOnlyIndicesMetadata(index1, index2))
                    .build()
            );
        });

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[] { "index1", "index2" });

        assertThat(response.totalDocCount, is(300L));
        assertThat(response.totalSizeInBytes, is(33L));
        assertThat(response.indexToStatsMap, aMapWithSize(2));
        assertThat(response.datastreamToStatsMap, aMapWithSize(1));
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(11L));
        assertThat(response.indexToStatsMap.get(index2.getName()).docCount(), is(200L));
        assertThat(response.datastreamToStatsMap.get("dataStream1").docCount(), is(200L));
        assertThat(response.datastreamToStatsMap.get("dataStream1").sizeInBytes(), is(22L));
        assertThat(response.indexToDatastreamMap, aMapWithSize(1));
        assertThat(response.indexToDatastreamMap.get(index2.getName()), equalTo("dataStream1"));
    }

    public void testNameFiltering() throws InterruptedException, TimeoutException {
        var action = createActionAndInitTransport(new CapturingTransport(), TimeValue.timeValueSeconds(5), true);

        var index1 = new Index("foo1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("foo2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        var index3 = new Index("bar", INDEX_UUID_NA_VALUE);
        var shardId3 = new ShardId(index3, 0);

        var index4 = new Index("baz", INDEX_UUID_NA_VALUE);
        var shardId4 = new ShardId(index4, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 10L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 20L).build()
        );
        when(mockShardsInfo.get(shardId3)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(300L, 30L, 0L, 30L).build()
        );
        when(mockShardsInfo.get(shardId4)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(400L, 40L, 0L, 40L).build()
        );

        var query = new String[] { "foo*", "bar*" };

        when(clusterMetricsService.getMeteringShardInfo()).thenReturn(mockShardsInfo);
        when(indexNameExpressionResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { "foo1", "foo2", "bar" });
        when(indexNameExpressionResolver.dataStreamNames(any(), any(), eq(query))).thenReturn(List.of());

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);
        when(task.isAssigned()).thenReturn(true);
        when(task.getExecutorNode()).thenReturn(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));
        createMockClusterState(clusterService, 3, 2, b -> {
            b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .add(addLocalOnlyIndexRouting(index3, shardId3))
                    .add(addLocalOnlyIndexRouting(index4, shardId4))
                    .build()
            );
            b.metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata)
                    .indices(createLocalOnlyIndicesMetadata(index1, index2, index3, index4))
                    .build()
            );
        });

        var request = new GetMeteringStatsAction.Request(query);
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        var response = listener.response;

        assertThat(response, notNullValue());

        assertThat(response.totalDocCount, is(600L));
        assertThat(response.totalSizeInBytes, is(60L));
        assertThat(response.indexToStatsMap, aMapWithSize(3));
        assertThat(response.datastreamToStatsMap, anEmptyMap());
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(10L));
    }

    public void testNameFilteringIncludesDatastreams() throws InterruptedException, TimeoutException {
        var action = createActionAndInitTransport(new CapturingTransport(), TimeValue.timeValueSeconds(5), true);

        var index1 = new Index("foo1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("foo2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        var index3 = new Index("bar", INDEX_UUID_NA_VALUE);
        var shardId3 = new ShardId(index3, 0);

        var dataStreamIndex = new Index(".ds-foo2", INDEX_UUID_NA_VALUE);
        var dsShardId = new ShardId(dataStreamIndex, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 10L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 20L).build()
        );
        when(mockShardsInfo.get(shardId3)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(300L, 30L, 0L, 30L).build()
        );
        when(mockShardsInfo.get(dsShardId)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(400L, 40L, 0L, 40L).build()
        );

        var query = new String[] { "foo*" };

        when(clusterMetricsService.getMeteringShardInfo()).thenReturn(mockShardsInfo);
        when(indexNameExpressionResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { "foo1", "foo2" });
        when(indexNameExpressionResolver.dataStreams(any(), any(), eq(query))).thenReturn(
            List.of(new IndexNameExpressionResolver.ResolvedExpression("fooDs", IndexComponentSelector.DATA))
        );

        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(true);
        when(task.getExecutorNode()).thenReturn(MockedClusterStateTestUtils.LOCAL_NODE_ID);

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        createMockClusterState(clusterService, 3, 2, b -> {
            b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .add(addLocalOnlyIndexRouting(index3, shardId3))
                    .add(addLocalOnlyIndexRouting(dataStreamIndex, dsShardId))
                    .build()
            );
            b.metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata)
                    .putCustom(
                        DataStreamMetadata.TYPE,
                        new DataStreamMetadata(
                            ImmutableOpenMap.<String, DataStream>builder()
                                .fPut("fooDs", DataStreamTestHelper.newInstance("fooDs", List.of(dataStreamIndex)))
                                .build(),
                            ImmutableOpenMap.of()
                        )
                    )
                    .indices(
                        createLocalOnlyIndicesMetadata(index1, index2, index3, dataStreamIndex)

                    )
                    .build()
            );
        });

        var request = new GetMeteringStatsAction.Request(query);
        var listener = new TestGetMeteringStatsActionResponseListener();

        action.doExecute(mock(Task.class), request, listener);
        listener.await();

        var response = listener.response;

        assertThat(response, notNullValue());

        assertThat(response.totalDocCount, is(700L));
        assertThat(response.totalSizeInBytes, is(70L));
        assertThat(response.indexToStatsMap, aMapWithSize(3));
        assertThat(response.datastreamToStatsMap, aMapWithSize(1));
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.datastreamToStatsMap.get("fooDs").docCount(), is(400L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(10L));
        assertThat(response.datastreamToStatsMap.get("fooDs").sizeInBytes(), is(40L));
    }

    public void testCreateResponseMultipleShards() {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), true);

        var index1 = new Index("index1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("index2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 11L).build()
        );
        when(mockShardsInfo.get(shardId2)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(200L, 20L, 0L, 22L).build()
        );

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index1)
                            .addIndexShard(
                                IndexShardRoutingTable.builder(shardId1)
                                    .addShard(
                                        TestShardRouting.newShardRouting(
                                            index1.getName(),
                                            shardId1.getId(),
                                            "node_0",
                                            true,
                                            ShardRoutingState.STARTED
                                        )
                                    )
                                    .addShard(
                                        TestShardRouting.newShardRouting(
                                            index1.getName(),
                                            shardId1.getId(),
                                            "node_1",
                                            false,
                                            ShardRoutingState.STARTED
                                        )
                                    )
                            )
                            .build()
                    )
                    .add(
                        IndexRoutingTable.builder(index2)
                            .addIndexShard(
                                IndexShardRoutingTable.builder(shardId2)
                                    .addShard(
                                        TestShardRouting.newShardRouting(
                                            index2.getName(),
                                            shardId2.getId(),
                                            "node_0",
                                            false,
                                            ShardRoutingState.STARTED
                                        )
                                    )
                                    .addShard(
                                        TestShardRouting.newShardRouting(
                                            index2.getName(),
                                            shardId2.getId(),
                                            "node_1",
                                            true,
                                            ShardRoutingState.STARTED
                                        )
                                    )
                            )
                            .build()
                    )
                    .build()
            )
        );

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[] { "index1", "index2" });

        assertThat(response.totalDocCount, is(300L));
        assertThat(response.totalSizeInBytes, is(33L));
        assertThat(response.indexToStatsMap, aMapWithSize(2));
        assertThat(response.indexToStatsMap.get(index1.getName()).docCount(), is(100L));
        assertThat(response.indexToStatsMap.get(index2.getName()).docCount(), is(200L));
        assertThat(response.indexToStatsMap.get(index1.getName()).sizeInBytes(), is(11L));
        assertThat(response.indexToStatsMap.get(index2.getName()).sizeInBytes(), is(22L));
    }

    public void testCreateResponseShardsWithoutInfo() {
        var transport = new CapturingTransport();
        var action = createActionAndInitTransport(transport, TimeValue.timeValueSeconds(5), false);

        var index1 = new Index("index1", INDEX_UUID_NA_VALUE);
        var shardId1 = new ShardId(index1, 0);

        var index2 = new Index("index2", INDEX_UUID_NA_VALUE);
        var shardId2 = new ShardId(index2, 0);

        SampledShardInfos mockShardsInfo = mock();
        when(mockShardsInfo.get(any())).thenReturn(ShardInfoMetrics.EMPTY);
        when(mockShardsInfo.get(shardId1)).thenReturn(
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(100L, 10L, 0L, 0).build()
        );

        createMockClusterState(
            clusterService,
            3,
            2,
            b -> b.routingTable(
                RoutingTable.builder()
                    .add(addLocalOnlyIndexRouting(index1, shardId1))
                    .add(addLocalOnlyIndexRouting(index2, shardId2))
                    .build()
            )
        );

        var response = action.createResponse(mockShardsInfo, clusterService.state(), new String[] { "index1", "index2" });

        assertThat(response.totalDocCount, is(100L));
        assertThat(response.totalSizeInBytes, is(10L));
    }

    public void testPersistentTaskNodeTransportActionTimeout() {
        var action = createActionAndInitTransport(new CapturingTransport(), MINIMUM_METERING_INFO_UPDATE_PERIOD, true);

        var timeout1 = action.getPersistentTaskNodeTransportActionTimeout();
        action.setMeteringShardInfoUpdatePeriod(TimeValue.ZERO);
        var timeout2 = action.getPersistentTaskNodeTransportActionTimeout();
        action.setMeteringShardInfoUpdatePeriod(TimeValue.timeValueSeconds(10));
        var timeout3 = action.getPersistentTaskNodeTransportActionTimeout();

        assertThat(timeout1.millis(), greaterThanOrEqualTo(TransportGetMeteringStatsAction.MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS));
        assertThat(timeout2.millis(), equalTo(TransportGetMeteringStatsAction.MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS));
        assertThat(timeout3.millis(), greaterThan(TransportGetMeteringStatsAction.MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS));
    }

    public void testInitialRetryBackoffPeriod() {
        var action = createActionAndInitTransport(new CapturingTransport(), TimeValue.timeValueSeconds(5), true);

        var period1 = action.getInitialRetryBackoffPeriod(TimeValue.ZERO);
        var period2 = action.getInitialRetryBackoffPeriod(TimeValue.timeValueSeconds(20));

        assertThat(period1.millis(), equalTo(TransportGetMeteringStatsAction.MINIMUM_INITIAL_BACKOFF_PERIOD_MILLIS));
        assertThat(period2.millis(), greaterThan(TransportGetMeteringStatsAction.MINIMUM_INITIAL_BACKOFF_PERIOD_MILLIS));
    }

    private static IndexRoutingTable addLocalOnlyIndexRouting(Index index1, ShardId shardId1) {
        return IndexRoutingTable.builder(index1)
            .addIndexShard(
                IndexShardRoutingTable.builder(shardId1)
                    .addShard(
                        TestShardRouting.newShardRouting(index1.getName(), shardId1.getId(), "node_0", true, ShardRoutingState.STARTED)
                    )
            )
            .build();
    }

    private Map<String, IndexMetadata> createLocalOnlyIndicesMetadata(Index... indices) {
        return Arrays.stream(indices)
            .map(
                index1 -> Map.entry(
                    index1.getName(),
                    IndexMetadata.builder(index1.getName())
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                        .numberOfReplicas(0)
                        .numberOfShards(1)
                        .build()
                )
            )
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static CapturingTransport createMockCapturingTransport(GetMeteringStatsAction.Response node1Response) {

        return new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                if (node.getId().equals("node_1")) {
                    handleResponse(requestId, node1Response);
                } else {
                    handleError(requestId, new TransportException("invalid node"));
                }
            }
        };
    }

    private static CapturingTransport createMockCapturingTransport(
        GetMeteringStatsAction.Response node1Response,
        GetMeteringStatsAction.Response node2Response
    ) {
        var invocations = new AtomicInteger(0);
        return new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                var invocation = invocations.addAndGet(1);
                if (invocation == 1) {
                    handleResponse(requestId, node1Response);
                } else {
                    handleResponse(requestId, node2Response);
                }
            }
        };
    }
}
