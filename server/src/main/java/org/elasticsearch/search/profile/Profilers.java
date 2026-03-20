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
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.dfs.DfsProfiler;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

/** Abstraction over all profilers for a single search request. */
public interface Profilers {

    /** Returns {@code true} when full Lucene instrumentation is enabled (ProfileWeight / ProfileScorer). */
    boolean isDetailed();

    QueryProfiler getCurrentQueryProfiler();

    AggregationProfiler getAggregationProfiler();

    DfsProfiler getDfsProfiler();

    void onQueryCollectorResult(CollectorResult result);

    FetchPhase.Profiler startProfilingFetchPhase();

    SearchProfileQueryPhaseResult buildQueryPhaseResults();
}
