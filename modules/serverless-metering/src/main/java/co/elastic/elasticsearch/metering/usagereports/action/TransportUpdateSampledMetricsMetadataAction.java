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

package co.elastic.elasticsearch.metering.usagereports.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class TransportUpdateSampledMetricsMetadataAction extends TransportMasterNodeAction<
    UpdateSampledMetricsMetadataAction.Request,
    ActionResponse.Empty> {

    private final SampledMetricsMetadataService sampledMetricsMetadataService;

    @Inject
    public TransportUpdateSampledMetricsMetadataAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            new SampledMetricsMetadataService(clusterService)
        );
    }

    TransportUpdateSampledMetricsMetadataAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Executor executor,
        SampledMetricsMetadataService sampledMetricsMetadataService
    ) {
        super(
            UpdateSampledMetricsMetadataAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateSampledMetricsMetadataAction.Request::new,
            indexNameExpressionResolver,
            in -> ActionResponse.Empty.INSTANCE,
            executor
        );
        this.sampledMetricsMetadataService = sampledMetricsMetadataService;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateSampledMetricsMetadataAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        try {
            sampledMetricsMetadataService.update(
                request.getMetadata(),
                request.masterNodeTimeout(),
                listener.map(unused -> ActionResponse.Empty.INSTANCE)
            );
        } catch (Exception ex) {
            assert false; // should not throw
            listener.onFailure(ex);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSampledMetricsMetadataAction.Request request, ClusterState state) {
        // we want users to be able to call this even when there are global blocks
        return null;
    }
}
