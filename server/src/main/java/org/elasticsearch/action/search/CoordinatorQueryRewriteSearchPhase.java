/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;

/**
 * This search phase can be used as an initial search phase to pre-filter search shards based on query rewriting.
 * The queries are rewritten against the shards and based on the rewrite result shards might be able to be excluded
 * from the search. The extra round trip to the search shards is very cheap and is not subject to rejections
 * which allows to fan out to more shards at the same time without running into rejections even if we are hitting a
 * large portion of the clusters indices.
 * This phase can also be used to pre-sort shards based on min/max values in each shard of the provided primary sort.
 * When the query primary sort is perform on a field, this phase extracts the min/max value in each shard and
 * sort them according to the provided order. This can be useful for instance to ensure that shards that contain recent
 * data are executed first when sorting by descending timestamp.
 */
final class CoordinatorQueryRewriteSearchPhase extends SearchPhase {

    private final Logger logger;
    private final SearchRequest request;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final ActionListener<SearchRequest> listener;

    private final CoordinatorRewriteContextProvider coordinatorRewriteContextProvider;

    private final IndicesService indicesService;

    private final Executor executor;

    CoordinatorQueryRewriteSearchPhase(
        Logger logger,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        Executor executor,
        IndicesService indicesService,
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider,
        ActionListener<SearchRequest> listener
    ) {
        super("coordinator_rewrite");

        this.logger = logger;
        this.request = request;
        this.executor = executor;
        this.listener = listener;
        this.shardsIts = shardsIts;
        this.coordinatorRewriteContextProvider = coordinatorRewriteContextProvider;
        this.indicesService = indicesService;
    }

    private static boolean assertSearchCoordinationThread() {
        return ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION);
    }

    @Override
    public void run() throws IOException {
        assert assertSearchCoordinationThread();
        runCoordinatorRewritePhase();
    }

    // tries to pre-filter shards based on information that's available to the coordinator
    // without having to reach out to the actual shards
    private void runCoordinatorRewritePhase() {
        // TODO: the index filter (i.e, `_index:patten`) should be prefiltered on the coordinator
        assert assertSearchCoordinationThread();
        final List<SearchShardIterator> matchedShardLevelRequests = new ArrayList<>();
        Map<String, Set<String>> fieldModelIds = new HashMap<>();
        for (SearchShardIterator searchShardIterator : shardsIts) {
            Index index = searchShardIterator.shardId().getIndex();
            MappingLookup mappingLookup = indicesService.indexService(index).mapperService().mappingLookup();
            mappingLookup.modelsForFields().forEach((k, v) -> {
                Set<String> modelIds = fieldModelIds.computeIfAbsent(k, value -> new HashSet<String>());
                modelIds.add(v);
            });
        }

        CoordinatorRewriteContext coordinatorRewriteContext = coordinatorRewriteContextProvider.getCoordinatorRewriteContextForModels(
            fieldModelIds
        );
        Rewriteable.rewriteAndFetch(request, coordinatorRewriteContext, listener);
    }

    @Override
    public void start() {
        // Note that the search is failed when this task is rejected by the executor
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(() -> format("Failed to execute [%s] while running [%s] phase", request, getName()), e);
                }
                listener.onFailure(new SearchPhaseExecutionException(getName(), e.getMessage(), e.getCause(), ShardSearchFailure.EMPTY_ARRAY));
            }

            @Override
            protected void doRun() throws IOException {
                CoordinatorQueryRewriteSearchPhase.this.run();
            }
        });
    }
}
