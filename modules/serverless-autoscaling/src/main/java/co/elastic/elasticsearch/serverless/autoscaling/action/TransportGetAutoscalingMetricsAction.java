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

import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction.Request;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction.Response;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.concurrent.atomic.AtomicReference;

public class TransportGetAutoscalingMetricsAction extends TransportMasterNodeAction<Request, Response> {
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetAutoscalingMetricsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            GetAutoscalingMetricsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        var parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        final AtomicReference<GetIndexTierMetrics.Response> indexTierMetricsRef = new AtomicReference<>();
        final AtomicReference<GetSearchTierMetrics.Response> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<GetMachineLearningTierMetrics.Response> machineLearningMetricsRef = new AtomicReference<>();
        ActionListener<Void> tierResponsesListener = listener.map(
            unused -> new Response(
                indexTierMetricsRef.get() != null ? indexTierMetricsRef.get().getMetrics() : null,
                searchTierMetricsRef.get() != null ? searchTierMetricsRef.get().getMetrics() : null,
                machineLearningMetricsRef.get() != null ? machineLearningMetricsRef.get().getMetrics() : null
            )
        );
        try (var refCountingListener = new RefCountingListener(tierResponsesListener)) {
            // TODO: add logging to each listener
            executeRequest(
                parentTaskAssigningClient,
                GetIndexTierMetrics.INSTANCE,
                new GetIndexTierMetrics.Request(request.timeout()),
                refCountingListener.acquire(indexTierMetricsRef::set)
            );
            executeRequest(
                parentTaskAssigningClient,
                GetSearchTierMetrics.INSTANCE,
                new GetSearchTierMetrics.Request(request.timeout()),
                refCountingListener.acquire(searchTierMetricsRef::set)
            );
            executeRequest(
                parentTaskAssigningClient,
                GetMachineLearningTierMetrics.INSTANCE,
                new GetMachineLearningTierMetrics.Request(request.timeout()),
                refCountingListener.acquire(machineLearningMetricsRef::set)
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    private static <Req extends ActionRequest, Resp extends ActionResponse> void executeRequest(
        Client client,
        ActionType<Resp> action,
        Req request,
        ActionListener<Resp> listener
    ) {
        ClientHelper.executeAsyncWithOrigin(
            client,
            // TODO: we might use our own origin
            ClientHelper.STACK_ORIGIN,
            action,
            request,
            listener
        );
    }
}
