/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Shard-level tracer for search execution. Captures timing spans for shard-level phases
 * (async_rewrite, lucene_rewrite, lucene_query, fetch, etc.) on the data node. The resulting
 * {@link SearchTraceResult.ShardTraceResult} is transported back to the coordinator and
 * attached to the appropriate coordinator phase span.
 *
 * <p>Like {@link SearchTracer}, this uses the NOOP pattern: call sites invoke methods
 * unconditionally, and the {@link #NOOP} singleton makes all calls free when tracing
 * is disabled.
 *
 * <p>This interface extends {@link Writeable} so that the tracer instance can be serialized
 * as part of shard-level requests sent from the coordinator to data nodes. On the data node,
 * the tracer captures spans and its {@link #buildResult()} produces the transportable result.
 */
public interface ShardSearchTracer extends Writeable {

    /**
     * No-op implementation used when tracing is disabled. All methods are empty and
     * {@link #buildResult()} returns null.
     */
    ShardSearchTracer NOOP = new ShardSearchTracer() {
        @Override
        public void setNodeAnchor(String nodeId, String nodeName) {}

        @Override
        public void startSpan(String name) {}

        @Override
        public void stopSpan(String name) {}

        @Override
        public void recordDetail(String key, Object value) {}

        @Override
        @Nullable
        public SearchTraceResult.ShardTraceResult buildResult() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(false);
        }
    };

    /**
     * Read a shard search tracer from a stream. Returns either an {@link ActiveShardSearchTracer}
     * (if the stream indicates tracing is active) or {@link #NOOP}.
     */
    static ShardSearchTracer readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new ActiveShardSearchTracer(in);
        }
        return NOOP;
    }

    /**
     * Set the node anchor for this shard tracer. Called on the data node when the shard
     * begins execution. Records the wall-clock anchor and starts the nano timer.
     *
     * @param nodeId   the data node's ID
     * @param nodeName the data node's name
     */
    void setNodeAnchor(String nodeId, String nodeName);

    /**
     * Start a new named span. Pushes onto the internal span stack.
     *
     * @param name the span name (e.g. "async_rewrite", "lucene_query", "fetch")
     */
    void startSpan(String name);

    /**
     * Stop the named span. Pops from the span stack and records the duration.
     *
     * @param name the span name, must match the most recently started span
     */
    void stopSpan(String name);

    /**
     * Record a detail on the currently open span. Details appear in the span's "details"
     * map in the output (e.g. slices, segments, query description).
     *
     * @param key   the detail key
     * @param value the detail value
     */
    void recordDetail(String key, Object value);

    /**
     * Build the final immutable shard trace result. Returns null if tracing produced no data.
     * Should be called exactly once after all shard-level phases have completed.
     *
     * @return the shard trace result, or null
     */
    @Nullable
    SearchTraceResult.ShardTraceResult buildResult();
}
