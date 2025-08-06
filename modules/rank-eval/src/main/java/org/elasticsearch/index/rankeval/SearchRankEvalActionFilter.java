/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;

public class SearchRankEvalActionFilter implements MappedActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchRankEvalActionFilter.class);
    private static final String RECALL_METRIC = "es.search.rankeval.recall.histogram";

    private final Client client;
    private final DoubleHistogram recallMetric;

    public SearchRankEvalActionFilter(Client client, MeterRegistry meterRegistry) {
        this.client = client;
        this.recallMetric = meterRegistry.registerDoubleHistogram(
            RECALL_METRIC,
            "Recall metric for rank evaluation, expressed as a histogram",
            "recall"
        );
    }

    @Override
    public String actionName() {
        return TransportSearchAction.NAME;
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        chain.proceed(task, action, request, listener.delegateFailureAndWrap((l, response) -> {
            l.onResponse(response);

            if (TransportSearchAction.NAME.equals(action)) {
                SearchRequest searchRequest = (SearchRequest) request;
                if (shouldCalculateRecall(searchRequest)) {
                    runRankEval(searchRequest, (SearchResponse) response);
                }
            }
        }));
    }

    private boolean shouldCalculateRecall(SearchRequest searchRequest) {
        return searchRequest.hasKnnSearch();
    }

    private void runRankEval(SearchRequest request, SearchResponse response) {
        // TODO set directly results instead of re-running search
        RatedRequest.RatingsProvider ratingsProvider = new RatedRequest.RatingsProvider(evalSourceBuilderFrom(request.source()));
        RatedRequest ratedRequest = new RatedRequest(String.valueOf(request.getRequestId()), request.source(), ratingsProvider);
        RankEvalSpec rankEvalSpec = new RankEvalSpec(List.of(ratedRequest), new RecallAtK());
        RankEvalRequest rankEvalRequest = new RankEvalRequest(rankEvalSpec, request.indices());

        try {
            client.execute(RankEvalPlugin.ACTION, rankEvalRequest, new ActionListener<RankEvalResponse>() {
                    @Override
                    public void onResponse(RankEvalResponse rankEvalResponse) {
                        double metricScore = rankEvalResponse.getMetricScore();
                        registerMetricScore(metricScore);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Error performing rank eval request", e);
                    }
                }
            );
        } catch (Exception e) {
            logger.error("Error retrieving rank eval results", e);
        }
    }

    private void registerMetricScore(double metricScore) {
        logger.debug("Metric score calculated: {}", metricScore);
        if (Double.isNaN(metricScore) == false) {
            // Record recall as a long (rounded to nearest int for histogram)
            recallMetric.record(metricScore);
        }
    }

    private SearchSourceBuilder evalSourceBuilderFrom(SearchSourceBuilder source) {
        List<KnnSearchBuilder> knnSearchBuilders = source.knnSearch();
        SearchSourceBuilder result = source.shallowCopy().knnSearch(List.of());

        // Combine all KNN queries into a disjunction
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (KnnSearchBuilder knnSearchBuilder : knnSearchBuilders) {
            boolQueryBuilder.should(
                new ExactKnnQueryBuilder(
                    knnSearchBuilder.getQueryVector(),
                    knnSearchBuilder.getField(),
                    knnSearchBuilder.getSimilarity()
                )
            );
        }
        if (source.query() != null) {
            // If there is an existing query, combine it with a disjunction with the knn queries
            boolQueryBuilder.should(source.query());
        }

        return result.query(boolQueryBuilder);
    }
}
