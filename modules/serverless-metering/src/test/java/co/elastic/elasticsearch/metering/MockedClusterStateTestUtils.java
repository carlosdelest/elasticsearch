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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedClusterStateTestUtils {
    private static final String TEST_CLUSTER = "test-cluster";

    public static final String LOCAL_NODE_ID = "node_0";
    public static final String NON_LOCAL_NODE_ID = "node_1";
    public static final long TASK_ALLOCATION_ID = 1L;

    public static ClusterState createMockClusterState() {
        int numberOfIndexNodes = ESTestCase.randomIntBetween(1, 3);
        int numberOfSearchNodes = ESTestCase.randomIntBetween(1, 3);
        return createMockClusterState(numberOfIndexNodes, numberOfSearchNodes, b -> {});
    }

    public static void createMockClusterState(ClusterService clusterService) {
        setState(clusterService, createMockClusterState());
    }

    public static ClusterState createMockClusterState(
        int numberOfIndexNodes,
        int numberOfSearchNodes,
        Consumer<ClusterState.Builder> clusterStateBuilderConsumer
    ) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();

        for (int i = 0; i < numberOfSearchNodes; i++) {
            final DiscoveryNode node = DiscoveryNodeUtils.builder("node_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
            discoBuilder = discoBuilder.add(node);
        }

        for (int i = numberOfSearchNodes; i < numberOfIndexNodes + numberOfSearchNodes; i++) {
            final DiscoveryNode node = DiscoveryNodeUtils.builder("node_" + i).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build();
            discoBuilder = discoBuilder.add(node);
        }

        discoBuilder.localNodeId(LOCAL_NODE_ID);
        discoBuilder.masterNodeId("node_" + (numberOfIndexNodes + numberOfSearchNodes - 1));
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName(TEST_CLUSTER));
        stateBuilder.nodes(discoBuilder);

        clusterStateBuilderConsumer.accept(stateBuilder);

        return stateBuilder.build();
    }

    public static void createMockClusterState(
        ClusterService clusterService,
        int numberOfIndexNodes,
        int numberOfSearchNodes,
        Consumer<ClusterState.Builder> clusterStateBuilderConsumer
    ) {
        setState(clusterService, createMockClusterState(numberOfIndexNodes, numberOfSearchNodes, clusterStateBuilderConsumer));
    }

    public static ClusterState createMockClusterStateWithPersistentTask(String nodeId) {
        ClusterState clusterState = createMockClusterState(2, 2, b -> {});
        return createMockClusterStateWithPersistentTask(clusterState, nodeId);
    }

    public static ClusterState createMockClusterStateWithPersistentTask(ClusterState currentState, String nodeId) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = mock(PersistentTasksCustomMetadata.PersistentTask.class);

        when(task.isAssigned()).thenReturn(true);
        when(task.getAllocationId()).thenReturn(TASK_ALLOCATION_ID);
        when(task.getExecutorNode()).thenReturn(nodeId);

        var taskMetadata = new PersistentTasksCustomMetadata(0L, Map.of(SampledClusterMetricsSchedulingTask.TASK_NAME, task));

        return ClusterState.builder(currentState)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, taskMetadata).build())
            .build();
    }

    public static void createMockClusterStateWithPersistentTask(
        ClusterService clusterService,
        int numberOfIndexNodes,
        int numberOfSearchNodes
    ) {
        var clusterState = createMockClusterStateWithPersistentTask(
            createMockClusterState(numberOfIndexNodes, numberOfSearchNodes, b -> {}),
            LOCAL_NODE_ID
        );
        setState(clusterService, clusterState);
    }

    public static void createMockClusterStateWithPersistentTask(ClusterService clusterService) {
        createMockClusterStateWithPersistentTask(clusterService, 2, 2);
    }
}
