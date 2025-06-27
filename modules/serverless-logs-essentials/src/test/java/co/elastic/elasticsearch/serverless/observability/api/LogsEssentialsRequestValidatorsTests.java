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
import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LogsEssentialsRequestValidatorsTests extends ESTestCase {
    public void testSyncRequestAggregations() {
        Settings settings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), ProjectType.OBSERVABILITY.name())
            .put(ServerlessSharedSettings.OBSERVABILITY_TIER.getKey(), ObservabilityTier.LOGS_ESSENTIALS.name())
            .build();

        LogsEssentialsSearchRequestValidator validator = new LogsEssentialsSearchRequestValidator(settings);
        verifyAggregations(TransportSearchAction.NAME, validator, aggName -> {
            SearchRequest request = new SearchRequest();
            SearchSourceBuilder sourceBuilder = createSearchSourceBuilderWithAgg(aggName);
            request.source(sourceBuilder);
            return request;
        });
    }

    public void testAsyncRequestAggregations() {
        Settings settings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), ProjectType.OBSERVABILITY.name())
            .put(ServerlessSharedSettings.OBSERVABILITY_TIER.getKey(), ObservabilityTier.LOGS_ESSENTIALS.name())
            .build();

        LogsEssentialsAsyncSearchRequestValidator validator = new LogsEssentialsAsyncSearchRequestValidator(settings);
        verifyAggregations(SubmitAsyncSearchAction.NAME, validator, aggName -> {
            SearchSourceBuilder sourceBuilder = createSearchSourceBuilderWithAgg(aggName);
            return new SubmitAsyncSearchRequest(sourceBuilder);
        });
    }

    private static SearchSourceBuilder createSearchSourceBuilderWithAgg(String aggName) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder prohibitedAgg = new TermsAggregationBuilder(aggName);
        prohibitedAgg.field("test_field");

        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_value");
        avgAgg.field("value");
        avgAgg.subAggregation(prohibitedAgg);

        sourceBuilder.aggregation(avgAgg);
        return sourceBuilder;
    }

    public void testDisabledWithOtherProjectTypes() {
        Settings settings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), ProjectType.ELASTICSEARCH_GENERAL_PURPOSE.name())
            .build();

        LogsEssentialsSearchRequestValidator validator = new LogsEssentialsSearchRequestValidator(settings);

        String aggregation = "frequent_item_sets";

        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder(aggregation);
        aggregationBuilder.field("test");
        sourceBuilder.aggregation(aggregationBuilder);

        request.source(sourceBuilder);

        validateRequest(TransportSearchAction.NAME, validator, request);
    }

    private static <Request extends ActionRequest> void verifyAggregations(
        String action,
        AbstractLogsEssentialsRequestValidator validator,
        Function<String, Request> requestCreator
    ) {
        assertThat(action, equalTo(validator.actionName()));

        String allowedAggregation = AvgAggregationBuilder.NAME;
        requestCreator.apply(allowedAggregation);

        Collection<String> prohibitedAggregations = List.of("categorize_text", "change_point", "frequent_item_sets");

        for (String prohibitedAggName : prohibitedAggregations) {
            Request request = requestCreator.apply(prohibitedAggName);

            ElasticsearchStatusException exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> validateRequest(action, validator, request)
            );

            assertThat(RestStatus.BAD_REQUEST, equalTo(exception.status()));
            assertThat("Aggregation [" + prohibitedAggName + "] is not available in current project tier", equalTo(exception.getMessage()));
        }
    }

    @SuppressWarnings("unchecked")
    private static <Request extends ActionRequest, Response extends ActionResponse> void validateRequest(
        String action,
        AbstractLogsEssentialsRequestValidator validator,
        Request request
    ) {
        Task task = mock(Task.class);
        ActionListener<Response> listener = mock(ActionListener.class);
        ActionFilterChain<Request, Response> chain = mock(ActionFilterChain.class);
        validator.apply(task, action, request, listener, chain);
        verify(chain).proceed(eq(task), eq(action), eq(request), eq(listener));
    }
}
