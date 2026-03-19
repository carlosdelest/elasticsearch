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
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.dfs.DfsProfiler;
import org.elasticsearch.search.profile.dfs.DfsTimingType;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lightweight profiler that records only total wall-clock time for each search phase (DFS, query, fetch).
 * Unlike {@link DetailedProfiler}, this implementation:
 * <ul>
 *   <li>Does not instrument Lucene internals (no {@code ProfileWeight} / {@code ProfileScorer} wrapping)</li>
 *   <li>Does not call {@link org.elasticsearch.search.internal.ContextIndexSearcher#setProfiler} and therefore
 *       does not interfere with concurrent sliced execution</li>
 * </ul>
 */
public final class TimingProfiler implements Profilers {

    private long dfsPhaseNanos = -1;
    private long queryPhaseNanos = -1;

    @Override
    public boolean isDetailed() {
        return false;
    }

    @Override
    public QueryProfiler getCurrentQueryProfiler() {
        return null;
    }

    @Override
    public AggregationProfiler getAggregationProfiler() {
        return null;
    }

    @Override
    public DfsProfiler getDfsProfiler() {
        return new TimingDfsProfiler();
    }

    @Override
    public void onDfsPhaseComplete(long nanos) {
        this.dfsPhaseNanos = nanos;
    }

    @Override
    public void onQueryPhaseComplete(long nanos) {
        this.queryPhaseNanos = nanos;
    }

    @Override
    public void onQueryCollectorResult(CollectorResult result) {
        // no-op: lightweight profiling does not track collector details
    }

    @Override
    public FetchPhase.Profiler startProfilingFetchPhase() {
        return new TimingFetchProfiler();
    }

    @Override
    public SearchProfileDfsPhaseResult buildDfsPhaseResult() {
        ProfileResult result = new ProfileResult(
            "dfs",
            "distributed frequency statistics",
            Map.of("time_in_nanos", dfsPhaseNanos),
            Map.of(),
            dfsPhaseNanos,
            List.of()
        );
        return new SearchProfileDfsPhaseResult(result, null);
    }

    @Override
    public SearchProfileQueryPhaseResult buildQueryPhaseResults() {
        ProfileResult queryResult = new ProfileResult(
            "query",
            "search query execution",
            Map.of("time_in_nanos", queryPhaseNanos),
            Map.of(),
            queryPhaseNanos,
            List.of()
        );
        CollectorResult emptyCollector = new CollectorResult(
            "lightweight",
            CollectorResult.REASON_SEARCH_QUERY_PHASE,
            0L,
            Collections.emptyList()
        );
        QueryProfileShardResult shardResult = new QueryProfileShardResult(
            Collections.singletonList(queryResult),
            0L,
            emptyCollector,
            null
        );
        return new SearchProfileQueryPhaseResult(
            Collections.singletonList(shardResult),
            new AggregationProfileShardResult(List.of())
        );
    }

    /** Lightweight fetch profiler: records only start/end wall-clock time for the entire fetch phase. */
    private static class TimingFetchProfiler implements FetchPhase.Profiler {

        private final long startNanos = System.nanoTime();

        @Override
        public ProfileResult finish() {
            long elapsed = System.nanoTime() - startNanos;
            return new ProfileResult(
                "fetch",
                "fetch phase execution",
                Map.of("time_in_nanos", elapsed),
                Map.of(),
                elapsed,
                List.of()
            );
        }

        @Override
        public FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor processor) {
            return processor;
        }

        @Override
        public StoredFieldLoader storedFields(StoredFieldLoader storedFieldLoader) {
            return storedFieldLoader;
        }

        @Override
        public Timer startLoadingSource() {
            return null;
        }

        @Override
        public Timer startNextReader() {
            return null;
        }
    }

    /**
     * A no-op {@link DfsProfiler} used by timing profilers so that the DFS profiler is never
     * {@code null} and the existing null-checks in statistics collection are preserved without change.
     */
    private static class TimingDfsProfiler extends DfsProfiler {

        private Timer timer = new Timer();

        @Override
        public void start() {
            timer.start();
        }

        @Override
        public void stop() {
            timer.stop();
        }

        @Override
        public Timer startTimer(DfsTimingType dfsTimingType) {
            return null;
        }

        @Override
        public QueryProfiler addQueryProfiler() {
            return new QueryProfiler();
        }

        @Override
        public SearchProfileDfsPhaseResult buildDfsPhaseResults() {
            return new SearchProfileDfsPhaseResult(null, null);
        }
    };
}
