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

import co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetMachineLearningTierMetrics.Request;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetMachineLearningTierMetrics.Response;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats;

public class TransportGetMachineLearningTierMetrics extends HandledTransportAction<Request, Response> {

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetMachineLearningTierMetrics(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(
            GetMachineLearningTierMetrics.NAME,
            false,
            transportService,
            actionFilters,
            Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        GetMlAutoscalingStats.Request autoscalingStatsRequest = new GetMlAutoscalingStats.Request(
            request.masterNodeTimeout(),
            request.requestTimeout()
        );

        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        Client parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        ClientHelper.executeAsyncWithOrigin(
            parentTaskAssigningClient,
            ClientHelper.ML_ORIGIN,
            GetMlAutoscalingStats.INSTANCE,
            autoscalingStatsRequest,
            ActionListener.wrap(response -> {
                ActionListener.completeWith(
                    listener,
                    () -> new Response(new MachineLearningTierMetrics(response.getAutoscalingResources()))
                );
            }, listener::onFailure)
        );
    }
}
