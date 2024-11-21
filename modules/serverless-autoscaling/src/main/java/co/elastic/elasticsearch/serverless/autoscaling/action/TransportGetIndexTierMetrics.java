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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetIndexTierMetrics extends TransportMasterNodeAction<GetIndexTierMetrics.Request, GetIndexTierMetrics.Response> {

    private final IngestMetricsService ingestMetricsService;

    @Inject
    public TransportGetIndexTierMetrics(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IngestMetricsService ingestMetricsService
    ) {
        super(
            GetIndexTierMetrics.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexTierMetrics.Request::new,
            indexNameExpressionResolver,
            GetIndexTierMetrics.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ingestMetricsService = ingestMetricsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetIndexTierMetrics.Request request,
        ClusterState state,
        ActionListener<GetIndexTierMetrics.Response> listener
    ) {
        ActionListener.completeWith(
            listener,
            () -> new GetIndexTierMetrics.Response(ingestMetricsService.getIndexTierMetrics(clusterService.state()))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTierMetrics.Request request, ClusterState state) {
        return null;
    }
}
