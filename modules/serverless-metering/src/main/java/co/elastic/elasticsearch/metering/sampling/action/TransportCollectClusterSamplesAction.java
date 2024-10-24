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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.NotPersistentTaskNodeException;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        super(CollectClusterSamplesAction.NAME, false, transportService, actionFilters, CollectClusterSamplesAction.Request::new, executor);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.executor = executor;
        this.coolDownPeriod = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
    }

    private static class SingleNodeResponse {
        private final GetNodeSamplesAction.Response response;
        private final Exception ex;

        SingleNodeResponse(GetNodeSamplesAction.Response response) {
            this.response = response;
            this.ex = null;
        }

        SingleNodeResponse(Exception ex) {
            this.response = null;
            this.ex = ex;
        }

        boolean isValid() {
            return ex == null;
        }

        Exception getFailure() {
            return ex;
        }

        GetNodeSamplesAction.Response getResponse() {
            return response;
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
        DiscoveryNodes nodes = clusterState.nodes();

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
        final int expectedOps = nodes.size();
        logger.trace("querying {} data nodes based on cluster state version [{}]", expectedOps, clusterState.version());

        if (expectedOps == 0) {
            ActionListener.completeWith(listener, TransportCollectClusterSamplesAction::buildEmptyResponse);
            return;
        }
        final AtomicInteger counterOps = new AtomicInteger();
        // Since we will not concurrently update individual entries (each node will update a single indexed reference) we do not need
        // a AtomicReferenceArray
        final SingleNodeResponse[] responses = new SingleNodeResponse[expectedOps];
        final AtomicLong searchTierMemory = new AtomicLong();
        final AtomicLong indexTierMemory = new AtomicLong();

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
                responses[nodeIndex] = new SingleNodeResponse(response);

                if (node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName())) {
                    searchTierMemory.addAndGet(response.getPhysicalMemorySize());
                } else if (node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName())) {
                    indexTierMemory.addAndGet(response.getPhysicalMemorySize());
                }

                if (expectedOps == counterOps.incrementAndGet()) {
                    ActionListener.completeWith(listener, () -> buildResponse(searchTierMemory.get(), indexTierMemory.get(), responses));
                }
            }, ex -> {
                logger.warn("error while sending GetNodeSamplesAction.Request to [{}]: {}", node.getId(), ex);
                responses[nodeIndex] = new SingleNodeResponse(ex);
                if (expectedOps == counterOps.incrementAndGet()) {
                    ActionListener.completeWith(listener, () -> buildResponse(searchTierMemory.get(), indexTierMemory.get(), responses));
                }
            }));
        }
    }

    private static CollectClusterSamplesAction.Response buildEmptyResponse() {
        return new CollectClusterSamplesAction.Response(0, 0, Activity.EMPTY, Activity.EMPTY, Map.of(), List.of());
    }

    private CollectClusterSamplesAction.Response buildResponse(
        long searchTierMemory,
        long indexTierMemory,
        SingleNodeResponse[] responses
    ) {
        var normalizedShards = Arrays.stream(responses)
            .filter(SingleNodeResponse::isValid)
            .map(SingleNodeResponse::getResponse)
            .flatMap(m -> m.getShardInfos().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, ShardInfoMetrics::mostRecent));

        var failures = Arrays.stream(responses)
            .filter(Predicate.not(SingleNodeResponse::isValid))
            .map(SingleNodeResponse::getFailure)
            .toList();

        var searchActivities = Arrays.stream(responses)
            .filter(SingleNodeResponse::isValid)
            .map(SingleNodeResponse::getResponse)
            .map(GetNodeSamplesAction.Response::getSearchActivity);
        Activity searchActivityMerged = Activity.merge(searchActivities, coolDownPeriod);

        var indexActivities = Arrays.stream(responses)
            .filter(SingleNodeResponse::isValid)
            .map(SingleNodeResponse::getResponse)
            .map(GetNodeSamplesAction.Response::getIndexActivity);
        Activity indexActivityMerged = Activity.merge(indexActivities, coolDownPeriod);

        return new CollectClusterSamplesAction.Response(
            searchTierMemory,
            indexTierMemory,
            searchActivityMerged,
            indexActivityMerged,
            normalizedShards,
            failures
        );
    }

    private void sendRequest(
        DiscoveryNode node,
        GetNodeSamplesAction.Request request,
        ActionListener<GetNodeSamplesAction.Response> listener
    ) {
        transportService.sendRequest(
            node,
            GetNodeSamplesAction.INSTANCE.name(),
            request,
            new ActionListenerResponseHandler<>(listener, GetNodeSamplesAction.Response::new, executor)
        );
    }
}
