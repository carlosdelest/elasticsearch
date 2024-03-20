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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Performs a scatter-gather operation towards all search nodes, collecting from each node a valid
 * {@link GetMeteringShardInfoAction.Response} or an error. Responses are consolidated in a {@link CollectMeteringShardInfoAction.Response}
 * containing all shards sizes in the form of a {@code Map<ShardId, MeteringShardInfo>}. When multiple nodes respond with size information
 * for the same shard (ShardId), we retain the most recent information (based on primary term and generation).
 */
public class TransportCollectMeteringShardInfoAction extends HandledTransportAction<
    CollectMeteringShardInfoAction.Request,
    CollectMeteringShardInfoAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportCollectMeteringShardInfoAction.class);
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Executor executor;

    @Inject
    public TransportCollectMeteringShardInfoAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        this(transportService, clusterService, actionFilters, threadPool.executor(ThreadPool.Names.MANAGEMENT));
    }

    TransportCollectMeteringShardInfoAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Executor executor
    ) {
        super(
            CollectMeteringShardInfoAction.NAME,
            false,
            transportService,
            actionFilters,
            CollectMeteringShardInfoAction.Request::new,
            executor
        );
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.executor = executor;
    }

    private static class SingleNodeResponse {
        private final Map<ShardId, MeteringShardInfo> response;
        private final Exception ex;

        SingleNodeResponse(Map<ShardId, MeteringShardInfo> response) {
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

        Map<ShardId, MeteringShardInfo> getResponse() {
            return response;
        }
    }

    @Override
    protected void doExecute(
        Task task,
        CollectMeteringShardInfoAction.Request request,
        ActionListener<CollectMeteringShardInfoAction.Response> listener
    ) {
        logger.debug("Executing TransportCollectMeteringShardInfoAction");
        var clusterState = clusterService.state();
        final Set<DiscoveryNode> nodes = clusterState.nodes()
            .stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toSet());

        final int expectedOps = nodes.size();
        logger.trace("querying {} data nodes based on cluster state version [{}]", expectedOps, clusterState.version());

        final AtomicInteger counterOps = new AtomicInteger();
        // Since we will not concurrently update individual entries (each node will update a single indexed reference) we do not need
        // a AtomicReferenceArray
        final SingleNodeResponse[] responses = new SingleNodeResponse[expectedOps];

        int i = 0;
        for (DiscoveryNode node : nodes) {
            final int nodeIndex = i++;
            var shardInfoRequest = new GetMeteringShardInfoAction.Request();
            shardInfoRequest.setParentTask(clusterService.localNode().getId(), task.getId());

            sendRequest(node, shardInfoRequest, ActionListener.wrap(response -> {
                logger.debug("received GetMeteringShardInfo response from [{}]", node.getId());
                responses[nodeIndex] = new SingleNodeResponse(response.getMeteringShardInfoMap());
                if (expectedOps == counterOps.incrementAndGet()) {
                    ActionListener.completeWith(listener, () -> buildResponse(responses));
                }
            }, ex -> {
                logger.warn("error while sending GetMeteringShardInfo.Request to [{}]: {}", node.getId(), ex);
                responses[nodeIndex] = new SingleNodeResponse(ex);
                if (expectedOps == counterOps.incrementAndGet()) {
                    ActionListener.completeWith(listener, () -> buildResponse(responses));
                }
            }));
        }
    }

    private CollectMeteringShardInfoAction.Response buildResponse(SingleNodeResponse[] responses) {
        var normalizedShards = Arrays.stream(responses)
            .filter(SingleNodeResponse::isValid)
            .map(SingleNodeResponse::getResponse)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TransportCollectMeteringShardInfoAction::mostRecent));

        var failures = Arrays.stream(responses)
            .filter(Predicate.not(SingleNodeResponse::isValid))
            .map(SingleNodeResponse::getFailure)
            .toList();
        return new CollectMeteringShardInfoAction.Response(normalizedShards, failures);
    }

    private static MeteringShardInfo mostRecent(MeteringShardInfo v1, MeteringShardInfo v2) {
        if ((v1.primaryTerm() > v2.primaryTerm()) || (v1.primaryTerm() == v2.primaryTerm() && v1.generation() > v2.generation())) {
            return v1;
        }
        return v2;
    }

    private void sendRequest(
        DiscoveryNode node,
        GetMeteringShardInfoAction.Request request,
        ActionListener<GetMeteringShardInfoAction.Response> listener
    ) {
        transportService.sendRequest(
            node,
            GetMeteringShardInfoAction.INSTANCE.name(),
            request,
            new ActionListenerResponseHandler<>(listener, GetMeteringShardInfoAction.Response::new, executor)
        );
    }
}
