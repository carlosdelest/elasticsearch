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

import co.elastic.elasticsearch.serverless.autoscaling.action.GetSearchTierMetrics.Request;
import co.elastic.elasticsearch.serverless.autoscaling.model.TierMetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static co.elastic.elasticsearch.serverless.autoscaling.model.Metric.exact;

public class TransportGetSearchTierMetrics extends HandledTransportAction<Request, TierMetricsResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportGetSearchTierMetrics(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(GetSearchTierMetrics.NAME, transportService, actionFilters, Request::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<TierMetricsResponse> listener) {
        TierMetrics metrics = new TierMetrics(Map.of("interactive_load", exact(10), "non_interactive_load", exact(0)));
        listener.onResponse(new TierMetricsResponse(GetSearchTierMetrics.TIER_NAME, metrics));
    }
}
