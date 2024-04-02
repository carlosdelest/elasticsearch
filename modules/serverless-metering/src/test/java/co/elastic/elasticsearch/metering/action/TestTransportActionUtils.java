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

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

class TestTransportActionUtils {
    private static final String TEST_CLUSTER = "test-cluster";

    static final String LOCAL_NODE_ID = "node_0";

    static void createMockClusterState(ClusterService clusterService) {
        int numberOfNodes = ESTestCase.randomIntBetween(3, 5);
        int numberOfSearchNodes = ESTestCase.randomIntBetween(1, numberOfNodes);
        createMockClusterState(clusterService, numberOfNodes, numberOfSearchNodes, b -> {});
    }

    static void createMockClusterState(
        ClusterService clusterService,
        int numberOfNodes,
        int numberOfSearchNodes,
        Consumer<ClusterState.Builder> clusterStateBuilderConsumer
    ) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();

        for (int i = 0; i < numberOfSearchNodes; i++) {
            final DiscoveryNode node = DiscoveryNodeUtils.builder("node_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
            discoBuilder = discoBuilder.add(node);
        }

        for (int i = numberOfSearchNodes; i < numberOfNodes; i++) {
            final DiscoveryNode node = DiscoveryNodeUtils.builder("node_" + i).roles(emptySet()).build();
            discoBuilder = discoBuilder.add(node);
        }

        discoBuilder.localNodeId(LOCAL_NODE_ID);
        discoBuilder.masterNodeId("node_" + (numberOfNodes - 1));
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName(TEST_CLUSTER));
        stateBuilder.nodes(discoBuilder);

        clusterStateBuilderConsumer.accept(stateBuilder);

        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    static MeteringShardInfo createMeteringShardInfo(ShardId shardId) {
        return new MeteringShardInfo(ESTestCase.randomLongBetween(0, 10000), ESTestCase.randomLongBetween(0, 10000), 0, 0);
    }

    static void awaitForkedTasks(ExecutorService executor) {
        PlainActionFuture.get(listener -> executor.execute(ActionRunnable.run(listener, () -> {})), 10, TimeUnit.SECONDS);
    }
}
