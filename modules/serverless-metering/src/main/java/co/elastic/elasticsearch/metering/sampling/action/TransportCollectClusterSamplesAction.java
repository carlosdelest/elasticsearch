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
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.NotPersistentTaskNodeException;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performs a scatter-gather operation towards all search nodes, collecting from each node a valid
 * {@link GetNodeSamplesAction.Response} or an error.
 *
 * Responses are consolidated in a {@link CollectClusterSamplesAction.Response} containing all sampled metrics
 * including shard sizes in the form of a {@code Map<ShardId, ShardInfoMetrics>}. When multiple nodes respond with size information
 * for the same shard (ShardId), we retain the most recent information (based on primary term and generation).
 */
public class TransportCollectClusterSamplesAction extends HandledTransportAction<
    CollectClusterSamplesAction.Request,
    CollectClusterSamplesAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportCollectClusterSamplesAction.class);
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Executor executor;
    private Duration coolDownPeriod;
    private TimeValue nodeSampleTimeout;

    public static final Setting<TimeValue> NODE_SAMPLE_TIMEOUT = Setting.timeSetting(
        "metering.node_sample_timeout",
        TimeValue.timeValueMinutes(3),
        Setting.Property.NodeScope
    );

    @SuppressWarnings("this-escape")
    @Inject
    public TransportCollectClusterSamplesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        this(transportService, clusterService, actionFilters, threadPool.executor(ThreadPool.Names.MANAGEMENT));
    }

    @SuppressWarnings("this-escape")
    TransportCollectClusterSamplesAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Executor executor
    ) {
        super(CollectClusterSamplesAction.NAME, false, transportService, actionFilters, in -> TransportAction.localOnly(), executor);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.executor = executor;
        this.coolDownPeriod = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
        this.nodeSampleTimeout = NODE_SAMPLE_TIMEOUT.get(clusterService.getSettings());
    }

    private static class SingleNodeResponse {
        private final boolean isSearchTier;
        private final boolean isIndexTier;
        private final GetNodeSamplesAction.Response success;
        private final Exception failure;

        SingleNodeResponse(DiscoveryNode node, GetNodeSamplesAction.Response success, Exception failure) {
            this.isSearchTier = node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName());
            this.isIndexTier = node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName());
            this.success = success;
            this.failure = failure;
            assert (success == null) != (failure == null);
        }
    }

    @Override
    protected void doExecute(
        Task task,
        CollectClusterSamplesAction.Request request,
        ActionListener<CollectClusterSamplesAction.Response> listener
    ) {
        logger.debug("Executing TransportCollectClusterSamplesAction");
        var clusterState = clusterService.state();

        var persistentTask = SampledClusterMetricsSchedulingTask.findTask(clusterState);
        if (persistentTask == null || persistentTask.isAssigned() == false) {
            listener.onFailure(new PersistentTaskNodeNotAssignedException(SampledClusterMetricsSchedulingTask.TASK_NAME));
            return;
        }
        if (clusterService.localNode().getId().equals(persistentTask.getExecutorNode()) == false) {
            listener.onFailure(
                new NotPersistentTaskNodeException(clusterService.localNode().getId(), SampledClusterMetricsSchedulingTask.TASK_NAME)
            );
            return;
        }
        var currentPersistentTaskAllocation = Long.toString(persistentTask.getAllocationId());

        final var nodes = clusterState.nodes()
            .stream()
            .filter(n -> n.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) || n.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
            .toList();
        final int expectedOps = nodes.size();
        logger.trace("querying {} data nodes based on cluster state version [{}]", expectedOps, clusterState.version());

        if (expectedOps == 0) {
            listener.onFailure(new IllegalStateException("Cluster state nodes empty"));
            return;
        }

        final AtomicInteger counterOps = new AtomicInteger();
        // Since we will not concurrently update individual entries (each node will update a single indexed reference) we do not need
        // a AtomicReferenceArray
        final SingleNodeResponse[] responses = new SingleNodeResponse[expectedOps];

        int i = 0;
        for (DiscoveryNode node : nodes) {
            final int nodeIndex = i++;
            var shardInfoRequest = new GetNodeSamplesAction.Request(
                currentPersistentTaskAllocation,
                request.getSearchActivity(),
                request.getIndexActivity()
            );
            shardInfoRequest.setParentTask(clusterService.localNode().getId(), task.getId());

            sendRequest(node, shardInfoRequest, ActionListener.wrap(response -> {
                logger.debug("received GetNodeSamplesAction response from [{}]", node.getId());
                responses[nodeIndex] = new SingleNodeResponse(node, response, null);
                if (expectedOps == counterOps.incrementAndGet()) {
                    respond(listener, responses, nodeIndex);
                }
            }, ex -> {
                logger.warn("error while sending GetNodeSamplesAction.Request to [{}]: {}", node.getId(), ex);
                responses[nodeIndex] = new SingleNodeResponse(node, null, ex);
                if (expectedOps == counterOps.incrementAndGet()) {
                    respond(listener, responses, nodeIndex);
                }
            }));
        }
    }

    private static CollectClusterSamplesAction.Response buildEmptyResponse() {
        return new CollectClusterSamplesAction.Response(0, 0, Activity.EMPTY, Activity.EMPTY, Map.of(), 1, 0, 1, 0);
    }

    private void respond(ActionListener<CollectClusterSamplesAction.Response> listener, SingleNodeResponse[] responses, int currentIndex) {
        Map<ShardId, ShardInfoMetrics> shardInfoMetrics = new HashMap<>();
        Activity.Merger searchActivity = new Activity.Merger();
        Activity.Merger indexActivity = new Activity.Merger();

        long searchMemory = 0;
        long indexMemory = 0;

        int searchNodes = 0;
        int searchNodeErrors = 0;
        int indexNodes = 0;
        int indexNodeErrors = 0;

        for (var nodeResponse : responses) {
            if (nodeResponse.success != null) {
                var response = nodeResponse.success;
                response.getShardInfos().forEach((id, shardInfo) -> shardInfoMetrics.merge(id, shardInfo, ShardInfoMetrics::mostRecent));
                searchActivity.add(response.getSearchActivity());
                indexActivity.add(response.getIndexActivity());
                if (nodeResponse.isSearchTier) {
                    searchMemory += response.getPhysicalMemorySize();
                    searchNodes++;
                } else if (nodeResponse.isIndexTier) {
                    indexMemory += response.getPhysicalMemorySize();
                    indexNodes++;
                }
            } else {
                if (nodeResponse.isSearchTier) {
                    searchNodes++;
                    searchNodeErrors++;
                } else if (nodeResponse.isIndexTier) {
                    indexNodes++;
                    indexNodeErrors++;
                }
            }
        }

        if (responses.length > searchNodeErrors + indexNodeErrors) {
            listener.onResponse(
                new CollectClusterSamplesAction.Response(
                    searchMemory,
                    indexMemory,
                    searchActivity.merge(coolDownPeriod),
                    indexActivity.merge(coolDownPeriod),
                    shardInfoMetrics,
                    searchNodes,
                    searchNodeErrors,
                    indexNodes,
                    indexNodeErrors
                )
            );
        } else {
            var exception = responses[currentIndex].failure;
            assert exception != null : "Expected exception for current index, all nodes have failed";
            for (var nodeResponse : responses) {
                if (exception != nodeResponse.failure) {
                    exception.addSuppressed(nodeResponse.failure);
                }
            }
            listener.onFailure(exception);
        }
    }

    private void sendRequest(
        DiscoveryNode node,
        GetNodeSamplesAction.Request request,
        ActionListener<GetNodeSamplesAction.Response> listener
    ) {
        // updates are scheduled on a fixed interval, set the request timeout to the poll interval to not risk
        // accumulating multiple pending updates in memory.
        transportService.sendRequest(
            node,
            GetNodeSamplesAction.INSTANCE.name(),
            request,
            TransportRequestOptions.timeout(nodeSampleTimeout),
            new ActionListenerResponseHandler<>(listener, GetNodeSamplesAction.Response::new, executor)
        );
    }
}
