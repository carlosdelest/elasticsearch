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
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetMachineLearningTierMetrics extends HandledTransportAction<Request, Response> {

    private final ClusterService clusterService;

    @Inject
    public TransportGetMachineLearningTierMetrics(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(GetMachineLearningTierMetrics.NAME, transportService, actionFilters, Request::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ActionListener.completeWith(
            listener,
            () -> new Response(new MachineLearningTierMetrics(1, 2048, 1024, 1, 0, 0, 0, 0, 0, MetricQuality.EXACT))
        );
    }
}
