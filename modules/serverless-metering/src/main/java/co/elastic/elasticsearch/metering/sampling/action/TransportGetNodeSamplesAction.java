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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetNodeSamplesAction extends HandledTransportAction<GetNodeSamplesAction.Request, GetNodeSamplesAction.Response> {
    private final ShardInfoMetricsReader shardMetricsReader;
    private final InMemoryShardInfoMetricsCache shardMetricsCache;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetNodeSamplesAction(
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        InMemoryShardInfoMetricsCache shardMetricsCache,
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
        this.shardMetricsReader = new ShardInfoMetricsReader(indicesService, telemetryProvider.getMeterRegistry());
        this.shardMetricsCache = shardMetricsCache;
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
            var shardSizes = shardMetricsReader.getUpdatedShardInfos(shardMetricsCache, request.getCacheToken());
            listener.onResponse(new GetNodeSamplesAction.Response(shardSizes));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
