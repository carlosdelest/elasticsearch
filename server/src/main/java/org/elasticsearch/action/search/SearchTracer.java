/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;

/**
 * Coordinator-level tracer for search execution. Captures timing spans for each search phase
 * (coordinator_rewrite, can_match, query, fetch, etc.) and assembles them into a span tree
 * suitable for Gantt chart rendering.
 *
 * <p>Instrumentation call sites use this interface unconditionally. When tracing is disabled,
 * the {@link #NOOP} singleton is used — all methods are empty, no timestamps are read,
 * and no objects are allocated.
 *
 * <p>The span tree is built using a stack-based model: {@link #startPhase} pushes a new span
 * onto the stack, and {@link #stopPhase} pops it off and attaches it as a child of the span
 * now on top. Shard results from data nodes are inserted under the appropriate coordinator
 * phase via {@link #attachShardResult}.
 */
public interface SearchTracer {

    /**
     * No-op implementation used when tracing is disabled. All methods are empty,
     * {@link #buildResult()} returns null, and {@link #shardTracer()} returns
     * {@link ShardSearchTracer#NOOP}.
     */
    SearchTracer NOOP = new SearchTracer() {
        @Override
        public void startPhase(String name) {}

        @Override
        public void stopPhase(String name) {}

        @Override
        public void attachShardResult(String shard, SearchTraceResult.ShardTraceResult result) {}

        @Override
        public void recordDetail(String key, Object value) {}

        @Override
        @Nullable
        public SearchTraceResult buildResult() {
            return null;
        }

        @Override
        public ShardSearchTracer shardTracer() {
            return ShardSearchTracer.NOOP;
        }
    };

    /**
     * Start a new named phase span. Pushes onto the internal span stack so that
     * subsequent calls to {@link #attachShardResult} or {@link #recordDetail}
     * operate on this phase.
     *
     * @param name the phase name (e.g. "coordinator_rewrite", "query", "fetch")
     */
    void startPhase(String name);

    /**
     * Stop the named phase span. Pops it from the span stack and attaches it as a child
     * of the enclosing phase (or the root span if at the top level).
     *
     * @param name the phase name, must match the most recently started phase
     */
    void stopPhase(String name);

    /**
     * Attach a shard-level trace result as children of the currently open phase.
     * The shard result's spans are inserted as children, and the shard's node anchor
     * is registered if not already present.
     *
     * @param shard  the shard identifier (e.g. "[node2][idx][0]")
     * @param result the shard trace result produced on the data node
     */
    void attachShardResult(String shard, SearchTraceResult.ShardTraceResult result);

    /**
     * Record a detail on the currently open phase span. Details are key-value pairs
     * that appear in the span's "details" map in the output (e.g. parallelism info,
     * partial reduce counts, query descriptions).
     *
     * @param key   the detail key
     * @param value the detail value (must be a type supported by generic value serialization)
     */
    void recordDetail(String key, Object value);

    /**
     * Build the final immutable trace result. Returns null if tracing produced no data.
     * Should be called exactly once after all phases have completed.
     *
     * @return the search trace result, or null
     */
    @Nullable
    SearchTraceResult buildResult();

    /**
     * Factory method to create a shard-level tracer. The returned tracer will be attached
     * to the {@link org.elasticsearch.search.internal.ShardSearchRequest} and used on the
     * data node to capture shard execution spans.
     *
     * @return a new shard tracer, or {@link ShardSearchTracer#NOOP} if tracing is disabled
     */
    ShardSearchTracer shardTracer();
}
