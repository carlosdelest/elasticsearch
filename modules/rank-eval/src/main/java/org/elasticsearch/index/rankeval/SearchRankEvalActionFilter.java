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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SearchRankEvalActionFilter implements MappedActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchRankEvalActionFilter.class);
    public static final String RECALL_METRIC = "es.search.rankeval.recall.histogram";
    public static final String RECALL_MEASURE_TIME_METRIC = "es.search.rankeval.recall.time.histogram";

    private final Client client;
    private final DoubleHistogram recallMetric;
    private final LongHistogram recallMeasurementTimeMetric;
    private AtomicLong numQueries;

    public SearchRankEvalActionFilter(Client client, MeterRegistry meterRegistry) {
        this.numQueries = new AtomicLong(0);
        this.client = client;
        this.recallMetric = meterRegistry.registerDoubleHistogram(
            RECALL_METRIC,
            "Recall metric for rank evaluation, expressed as a histogram",
            "recall"
        );
        recallMeasurementTimeMetric = meterRegistry.registerLongHistogram(RECALL_MEASURE_TIME_METRIC, "Time taken to measure recall", "ms");
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
        chain.proceed(task, action, request, new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                if (TransportSearchAction.NAME.equals(action)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    if (shouldCalculateRecall(searchRequest)) {
                        SearchResponse searchResponse = (SearchResponse) response;
                        SearchHits hits = searchResponse.getHits().asUnpooled();
                        runRankEval(searchRequest, hits);
                    }
                }
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private boolean shouldCalculateRecall(SearchRequest searchRequest) {
        boolean hasKnn = searchRequest.hasKnnSearch() || hasKnnQuery(searchRequest.source().query());
        return hasKnn && numQueries.getAndIncrement() % 100 == 0;
    }

    private boolean hasKnnQuery(QueryBuilder query) {
        if (query == null) {
            return false;
        }
        if (query instanceof KnnVectorQueryBuilder) {
            return true;
        }
        if (query instanceof BoolQueryBuilder boolQuery) {
            return boolQuery.must().stream().anyMatch(this::hasKnnQuery)
                || boolQuery.should().stream().anyMatch(this::hasKnnQuery)
                || boolQuery.filter().stream().anyMatch(this::hasKnnQuery)
                || boolQuery.mustNot().stream().anyMatch(this::hasKnnQuery);
        }
        return false;
    }

    private void runRankEval(SearchRequest request, SearchHits response) {
        try {
            long start = System.currentTimeMillis();
            RatedRequest.RatingsProvider ratingsProvider = new RatedRequest.RatingsProvider(evalSourceBuilderFrom(request.source()));
            RatedRequest ratedRequest = new RatedRequest(String.valueOf(request.getRequestId()), List.of(), ratingsProvider, response);
            RankEvalSpec rankEvalSpec = new RankEvalSpec(List.of(ratedRequest), new RecallAtK());
            RankEvalRequest rankEvalRequest = new RankEvalRequest(rankEvalSpec, request.indices());

            client.execute(RankEvalPlugin.ACTION, rankEvalRequest, new ActionListener<RankEvalResponse>() {
                @Override
                public void onResponse(RankEvalResponse rankEvalResponse) {
                    double metricScore = rankEvalResponse.getMetricScore();
                    registerMetricScore(metricScore, System.currentTimeMillis() - start);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Error performing rank eval request", e);
                }
            });
        } catch (Exception e) {
            logger.error("Error retrieving rank eval results", e);
        }
    }

    private void registerMetricScore(double metricScore, long measurementTime) {
        if (Double.isNaN(metricScore) == false) {
            recallMetric.record(metricScore);
        }
        recallMeasurementTimeMetric.record(measurementTime);
    }

    private QueryBuilder rewriteQuery(QueryBuilder query) {
        if (query == null) {
            return null;
        }
        if (query instanceof KnnVectorQueryBuilder knnQuery) {
            return new ExactKnnQueryBuilder(knnQuery.queryVector(), knnQuery.getFieldName(), knnQuery.getVectorSimilarity());
        }
        if (query instanceof BoolQueryBuilder boolQuery) {
            BoolQueryBuilder newBool = new BoolQueryBuilder();
            boolQuery.must().stream().map(this::rewriteQuery).forEach(newBool::must);
            boolQuery.should().stream().map(this::rewriteQuery).forEach(newBool::should);
            boolQuery.filter().stream().map(this::rewriteQuery).forEach(newBool::filter);
            boolQuery.mustNot().stream().map(this::rewriteQuery).forEach(newBool::mustNot);
            newBool.boost(boolQuery.boost());
            newBool.queryName(boolQuery.queryName());
            if (boolQuery.minimumShouldMatch() != null) {
                newBool.minimumShouldMatch(boolQuery.minimumShouldMatch());
            }
            return newBool;
        }
        return query;
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
            boolQueryBuilder.should(rewriteQuery(source.query()));
        }

        return result.query(boolQueryBuilder);
    }
}
