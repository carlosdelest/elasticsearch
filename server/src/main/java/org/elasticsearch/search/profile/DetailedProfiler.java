/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchProfiler;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.dfs.DfsProfiler;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.Collections;

/** Full profiler that instruments Lucene internals and records per-query, per-aggregation, and per-fetch-subphase timings. */
public final class DetailedProfiler implements Profilers {

    private final QueryProfiler queryProfiler;
    private final AggregationProfiler aggProfiler = new AggregationProfiler();
    private DfsProfiler dfsProfiler;

    public DetailedProfiler(ContextIndexSearcher searcher) {
        this.queryProfiler = new QueryProfiler();
        searcher.setProfiler(this.queryProfiler);
    }

    @Override
    public boolean isDetailed() {
        return true;
    }

    /** Get the profiler for the query we are currently processing. */
    @Override
    public QueryProfiler getCurrentQueryProfiler() {
        return queryProfiler;
    }

    @Override
    public AggregationProfiler getAggregationProfiler() {
        return aggProfiler;
    }

    /** Build a profiler for the dfs phase or get the existing one. */
    @Override
    public DfsProfiler getDfsProfiler() {
        if (dfsProfiler == null) {
            dfsProfiler = new DfsProfiler();
        }
        return dfsProfiler;
    }

    @Override
    public void onDfsPhaseComplete(long nanos) {
        // DfsProfiler tracks sub-timings internally; no wall-clock total needed here
    }

    @Override
    public void onQueryPhaseComplete(long nanos) {
        // QueryProfiler tracks sub-timings internally; no wall-clock total needed here
    }

    @Override
    public void onQueryCollectorResult(CollectorResult result) {
        queryProfiler.setCollectorResult(result);
    }

    /** Build a profiler for the fetch phase. */
    @Override
    public FetchPhase.Profiler startProfilingFetchPhase() {
        return new FetchProfiler();
    }

    @Override
    public SearchProfileDfsPhaseResult buildDfsPhaseResult() {
        return getDfsProfiler().buildDfsPhaseResults();
    }

    /** Build the results for the query phase. */
    @Override
    public SearchProfileQueryPhaseResult buildQueryPhaseResults() {
        QueryProfileShardResult result = new QueryProfileShardResult(
            queryProfiler.getTree(),
            queryProfiler.getRewriteTime(),
            queryProfiler.getCollectorResult(),
            null
        );
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new SearchProfileQueryPhaseResult(Collections.singletonList(result), aggResults);
    }
}
