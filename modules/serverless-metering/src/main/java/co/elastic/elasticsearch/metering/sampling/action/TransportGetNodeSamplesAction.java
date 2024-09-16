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

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetNodeSamplesAction extends HandledTransportAction<GetNodeSamplesAction.Request, GetNodeSamplesAction.Response> {
    private final ShardInfoMetricsReader shardMetricsReader;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetNodeSamplesAction(
        Settings settings,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ShardSizeStatsReader shardSizeStatsReader,
        TelemetryProvider telemetryProvider
    ) {
        super(
            GetNodeSamplesAction.NAME,
            false,
            transportService,
            actionFilters,
            GetNodeSamplesAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        // only gather shard metrics on search nodes
        this.shardMetricsReader = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)
            ? new ShardInfoMetricsReader.DefaultShardInfoMetricsReader(
                indicesService,
                shardSizeStatsReader,
                telemetryProvider.getMeterRegistry()
            )
            : new ShardInfoMetricsReader.NoOpReader();
        // TODO remove registration under legacy name once fully deployed
        transportService.registerRequestHandler(
            GetNodeSamplesAction.LEGACY_NAME,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            false,
            false,
            GetNodeSamplesAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, GetNodeSamplesAction.Request request, ActionListener<GetNodeSamplesAction.Response> listener) {
        try {
            var shardSizes = shardMetricsReader.getUpdatedShardInfos(request.getCacheToken());
            listener.onResponse(new GetNodeSamplesAction.Response(shardSizes));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
