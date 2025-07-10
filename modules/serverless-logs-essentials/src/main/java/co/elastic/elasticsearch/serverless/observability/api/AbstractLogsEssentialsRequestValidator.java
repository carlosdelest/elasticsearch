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

package co.elastic.elasticsearch.serverless.observability.api;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;

import java.util.Set;

abstract class AbstractLogsEssentialsRequestValidator implements MappedActionFilter {
    private static final Set<String> PROHIBITED_AGGREGATIONS = Set.of("categorize_text", "change_point", "frequent_item_sets");
    private final boolean logsEssentialProject;

    protected AbstractLogsEssentialsRequestValidator(Settings settings) {
        this.logsEssentialProject = ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings) == ObservabilityTier.LOGS_ESSENTIALS;
    }

    protected abstract SearchSourceBuilder getSource(ActionRequest request);

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (logsEssentialProject) {
            AggregatorFactories.Builder aggregations = getSource(request).aggregations();
            if (aggregations != null) {
                for (AggregationBuilder aggBuilder : aggregations.getAggregatorFactories()) {
                    validateAggregationsRecursively(aggBuilder);
                }
                for (PipelineAggregationBuilder aggBuilder : aggregations.getPipelineAggregatorFactories()) {
                    validateAggregation(aggBuilder);
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void validateAggregationsRecursively(AggregationBuilder aggregationBuilder) {
        validateAggregation(aggregationBuilder);

        for (AggregationBuilder subAggregation : aggregationBuilder.getSubAggregations()) {
            validateAggregationsRecursively(subAggregation);
        }
    }

    private static void validateAggregation(BaseAggregationBuilder baseAggregationBuilder) {
        String aggregationType = baseAggregationBuilder.getType();
        if (PROHIBITED_AGGREGATIONS.contains(aggregationType)) {
            String message = "Aggregation [" + aggregationType + "] is not available in current project tier";
            throw new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
        }
    }
}
