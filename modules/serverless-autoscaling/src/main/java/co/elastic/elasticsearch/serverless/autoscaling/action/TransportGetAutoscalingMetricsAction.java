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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.core.Tuple.tuple;

public class TransportGetAutoscalingMetricsAction extends HandledTransportAction<Request, Response> {
    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingMetricsAction.class);

    static final List<Tuple<ActionType<TierMetricsResponse>, Function<TimeValue, TierMetricsRequest>>> TIER_ACTIONS = List.of(
        tuple(GetMachineLearningTierMetrics.INSTANCE, GetMachineLearningTierMetrics.Request::new),
        tuple(GetIndexingTierMetrics.INSTANCE, GetIndexingTierMetrics.Request::new),
        tuple(GetSearchTierMetrics.INSTANCE, GetSearchTierMetrics.Request::new)
    );

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetAutoscalingMetricsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(GetAutoscalingMetricsAction.NAME, transportService, actionFilters, Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        GroupedActionListener<TierMetricsResponse> groupedActionListener = new GroupedActionListener<>(
            TIER_ACTIONS.size(),
            ActionListener.wrap(tierResponses -> {
                logger.debug("got {} responses", tierResponses.size());
                listener.onResponse(new Response(tierResponses.stream().toList()));
            }, listener::onFailure)
        );

        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        Client parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        TIER_ACTIONS.forEach(tier -> {
            ClientHelper.executeAsyncWithOrigin(
                parentTaskAssigningClient,
                // TODO: we might use our own origin
                ClientHelper.STACK_ORIGIN,
                tier.v1(),
                tier.v2().apply(request.timeout()),
                groupedActionListener
            );
        });
    }
}
