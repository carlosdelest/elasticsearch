/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.createParser;
import static org.elasticsearch.index.rankeval.RatedRequest.validateEvaluatedQuery;

/**
 * Instances of this class execute a collection of search intents (read: user
 * supplied query parameters) against a set of possible search requests (read:
 * search specifications, expressed as query/search request templates) and
 * compares the result against a set of annotated documents per search intent.
 *
 * If any documents are returned that haven't been annotated the document id of
 * those is returned per search intent.
 *
 * The resulting search quality is computed in terms of precision at n and
 * returned for each search specification for the full set of search intents as
 * averaged precision at n.
 */
public class TransportRankEvalAction extends HandledTransportAction<RankEvalRequest, RankEvalResponse> {
    private final Client client;
    private final ScriptService scriptService;
    private final NamedXContentRegistry namedXContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    @Inject
    public TransportRankEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        super(RankEvalPlugin.ACTION.name(), transportService, actionFilters, RankEvalRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.scriptService = scriptService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RankEvalRequest request, ActionListener<RankEvalResponse> listener) {
        RankEvalSpec evaluationSpecification = request.getRankEvalSpec();
        EvaluationMetric metric = evaluationSpecification.getMetric();

        List<RatedRequest> ratedRequests = evaluationSpecification.getRatedRequests();
        Map<String, Exception> errors = new ConcurrentHashMap<>(ratedRequests.size());

        Map<String, TemplateScript.Factory> scriptsWithoutParams = new HashMap<>();
        for (Entry<String, Script> entry : evaluationSpecification.getTemplates().entrySet()) {
            scriptsWithoutParams.put(entry.getKey(), scriptService.compile(entry.getValue(), TemplateScript.CONTEXT));
        }

        // --- Begin RatingsProvider logic ---
        List<RatedRequest> resolvedRatedRequests = new ArrayList<>(ratedRequests.size());
        ActionListener<RankEvalResponse> finalListener = listener;

        // Helper to continue with main logic after all ratings providers are resolved
        Runnable compareResults = () -> {
            compareResults(request, evaluationSpecification, resolvedRatedRequests, scriptsWithoutParams, errors, metric, finalListener);
        };

        // Check if any requests have a RatingsProvider
        try (RefCountingRunnable refCounter = new RefCountingRunnable(compareResults)) {
            for (RatedRequest ratedRequest : ratedRequests) {
                RatedRequest.RatingsProvider provider = ratedRequest.getRatingsProvider();
                if (provider == null) {
                    resolvedRatedRequests.add(ratedRequest);
                    continue;
                }
                retrieveRatedDocs(
                    ratedRequest,
                    request,
                    provider,
                    scriptsWithoutParams,
                    resolvedRatedRequests,
                    refCounter.acquire(),
                    errors
                );

            }
        }
    }

    private void retrieveRatedDocs(
        RatedRequest ratedRequest,
        RankEvalRequest request,
        RatedRequest.RatingsProvider provider,
        Map<String, TemplateScript.Factory> scriptsWithoutParams,
        List<RatedRequest> resolvedRatedRequests,
        Releasable onFinish,
        Map<String, Exception> errors
    ) {
        // Build the provider's query from template
        TemplateScript.Factory providerScript = scriptsWithoutParams.get(provider.getTemplateId());
        if (providerScript == null) {
            errors.put(
                ratedRequest.getId(),
                new IllegalArgumentException("Unknown ratings_provider template_id: " + provider.getTemplateId())
            );
            resolvedRatedRequests.add(ratedRequest);
            onFinish.close();
            return;
        }
        String providerRequest = providerScript.newInstance(provider.getParams()).execute();
        SearchSourceBuilder providerSource = null;
        try (
            XContentParser providerParser = createParser(
                namedXContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                new BytesArray(providerRequest),
                XContentType.JSON
            )
        ) {
            providerSource = new SearchSourceBuilder().parseXContent(providerParser, false, clusterSupportsFeature);
        } catch (IOException e) {
            errors.put(ratedRequest.getId(), e);
            resolvedRatedRequests.add(ratedRequest);
            onFinish.close();
            return;
        }

        SearchRequest providerSearchRequest = new SearchRequest(request.indices(), providerSource);
        providerSearchRequest.indicesOptions(request.indicesOptions());
        providerSearchRequest.searchType(request.searchType());

        client.search(providerSearchRequest, ActionListener.wrap(
            providerResponse -> {
                try (onFinish) {
                    List<RatedDocument> docs = new ArrayList<>();
                    for (SearchHit hit : providerResponse.getHits().getHits()) {
                        docs.add(new RatedDocument(hit.getIndex(), hit.getId(), 1));
                    }
                    // Build a new RatedRequest with these docs, but keep all other fields
                    RatedRequest resolved = new RatedRequest(
                        ratedRequest.getId(),
                        docs,
                        ratedRequest.getEvaluationRequest(),
                        ratedRequest.getParams(),
                        ratedRequest.getTemplateId(),
                        null // ratingsProvider already resolved
                    );
                    resolved.addSummaryFields(ratedRequest.getSummaryFields());
                    resolvedRatedRequests.add(resolved);
                }
            },
            ex -> {
                try (onFinish) {
                    errors.put(ratedRequest.getId(), ex);
                    resolvedRatedRequests.add(ratedRequest);
                }
            }
        ));
    }

    private void compareResults(
        RankEvalRequest request,
        RankEvalSpec evaluationSpecification,
        List<RatedRequest> resolvedRatedRequests,
        Map<String, TemplateScript.Factory> scriptsWithoutParams,
        Map<String, Exception> errors,
        EvaluationMetric metric,
        ActionListener<RankEvalResponse> finalListener
    ) {
        MultiSearchRequest msearchRequest = new MultiSearchRequest();
        msearchRequest.maxConcurrentSearchRequests(evaluationSpecification.getMaxConcurrentSearches());
        List<RatedRequest> ratedRequestsInSearch = new ArrayList<>();
        for (RatedRequest ratedRequest : resolvedRatedRequests) {
            SearchSourceBuilder evaluationRequest = ratedRequest.getEvaluationRequest();
            if (evaluationRequest == null) {
                Map<String, Object> params = ratedRequest.getParams();
                String templateId = ratedRequest.getTemplateId();
                TemplateScript.Factory templateScript = scriptsWithoutParams.get(templateId);
                String resolvedRequest = templateScript.newInstance(params).execute();
                try (
                    XContentParser subParser = createParser(
                        namedXContentRegistry,
                        LoggingDeprecationHandler.INSTANCE,
                        new BytesArray(resolvedRequest),
                        XContentType.JSON
                    )
                ) {
                    evaluationRequest = new SearchSourceBuilder().parseXContent(subParser, false, clusterSupportsFeature);
                    // check for parts that should not be part of a ranking evaluation request
                    validateEvaluatedQuery(evaluationRequest);
                } catch (IOException e) {
                    // if we fail parsing, put the exception into the errors map and continue
                    errors.put(ratedRequest.getId(), e);
                    continue;
                }
            }

            if (metric.forcedSearchSize().isPresent()) {
                evaluationRequest.size(metric.forcedSearchSize().getAsInt());
            }

            ratedRequestsInSearch.add(ratedRequest);
            List<String> summaryFields = ratedRequest.getSummaryFields();
            if (summaryFields.isEmpty()) {
                evaluationRequest.fetchSource(false);
            } else {
                evaluationRequest.fetchSource(summaryFields.toArray(new String[summaryFields.size()]), new String[0]);
            }
            SearchRequest searchRequest = new SearchRequest(request.indices(), evaluationRequest);
            searchRequest.indicesOptions(request.indicesOptions());
            searchRequest.searchType(request.searchType());
            msearchRequest.add(searchRequest);
        }
        assert ratedRequestsInSearch.size() == msearchRequest.requests().size();
        client.multiSearch(
            msearchRequest,
            new RankEvalActionListener(
                finalListener,
                metric,
                ratedRequestsInSearch.toArray(new RatedRequest[ratedRequestsInSearch.size()]),
                errors
            )
        );
    }

    static class RankEvalActionListener extends DelegatingActionListener<MultiSearchResponse, RankEvalResponse> {

        private final RatedRequest[] specifications;

        private final Map<String, Exception> errors;
        private final EvaluationMetric metric;

        RankEvalActionListener(
            ActionListener<RankEvalResponse> listener,
            EvaluationMetric metric,
            RatedRequest[] specifications,
            Map<String, Exception> errors
        ) {
            super(listener);
            this.metric = metric;
            this.errors = errors;
            this.specifications = specifications;
        }

        @Override
        public void onResponse(MultiSearchResponse multiSearchResponse) {
            int responsePosition = 0;
            Map<String, EvalQueryQuality> responseDetails = Maps.newMapWithExpectedSize(specifications.length);
            for (Item response : multiSearchResponse.getResponses()) {
                RatedRequest specification = specifications[responsePosition];
                if (response.isFailure() == false) {
                    SearchHit[] hits = response.getResponse().getHits().getHits();
                    EvalQueryQuality queryQuality = this.metric.evaluate(specification.getId(), hits, specification.getRatedDocs());
                    responseDetails.put(specification.getId(), queryQuality);
                } else {
                    errors.put(specification.getId(), response.getFailure());
                }
                responsePosition++;
            }
            delegate.onResponse(new RankEvalResponse(this.metric.combine(responseDetails.values()), responseDetails, this.errors));
        }
    }
}
