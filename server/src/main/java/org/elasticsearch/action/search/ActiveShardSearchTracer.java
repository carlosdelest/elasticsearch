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
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Active implementation of {@link ShardSearchTracer} that captures real timing data on a
 * data node for a single shard. Uses a stack-based span model to build nested shard-level spans.
 *
 * <p>The node anchor ({@link #setNodeAnchor}) must be called before any spans are started.
 * This establishes the per-node wall-clock and nano anchors used to compute span offsets.
 *
 * <p>Thread safety: this class is NOT thread-safe. It is designed to be used from the
 * search thread handling a single shard request.
 */
public final class ActiveShardSearchTracer implements ShardSearchTracer {

    private String nodeId;
    private String nodeName;
    private long wallClockAnchorMillis;
    private long nanoAnchor;
    private boolean anchorSet;

    private final Deque<SpanBuilder> spanStack;
    private final List<TraceSpan> completedSpans;

    public ActiveShardSearchTracer() {
        this.spanStack = new ArrayDeque<>();
        this.completedSpans = new ArrayList<>();
    }

    /**
     * Reconstruct from a stream. Used when deserializing a shard request on the data node.
     */
    ActiveShardSearchTracer(StreamInput in) throws IOException {
        // The boolean marker has already been read by ShardSearchTracer.readFrom()
        this.spanStack = new ArrayDeque<>();
        this.completedSpans = new ArrayList<>();
        // No state to read beyond the active marker — the tracer starts empty
        // and captures spans during shard execution on the data node.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(true);
        // No additional state to write — the tracer is a marker indicating
        // that tracing is active. Span data is captured on the data node
        // and returned via buildResult().
    }

    @Override
    public void setNodeAnchor(String nodeId, String nodeName) {
        if (false == anchorSet) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.wallClockAnchorMillis = System.currentTimeMillis();
            this.nanoAnchor = System.nanoTime();
            this.anchorSet = true;
        }
    }

    @Override
    public void startSpan(String name) {
        if (false == anchorSet) {
            return;
        }
        long offsetNanos = System.nanoTime() - nanoAnchor;
        SpanBuilder span = new SpanBuilder(name);
        span.startOffsetNanos = offsetNanos;
        spanStack.push(span);
    }

    @Override
    public void stopSpan(String name) {
        if (false == anchorSet) {
            return;
        }
        long offsetNanos = System.nanoTime() - nanoAnchor;
        SpanBuilder top = spanStack.peek();
        if (top == null || false == top.name.equals(name)) {
            return;
        }
        spanStack.pop();
        top.stopOffsetNanos = offsetNanos;
        TraceSpan completed = top.build();

        SpanBuilder parent = spanStack.peek();
        if (parent != null) {
            parent.children.add(completed);
        } else {
            completedSpans.add(completed);
        }
    }

    @Override
    public void recordDetail(String key, Object value) {
        SpanBuilder currentSpan = spanStack.peek();
        if (currentSpan != null) {
            currentSpan.details.put(key, value);
        }
    }

    @Override
    public void attachSpan(TraceSpan span) {
        if (false == anchorSet) {
            return;
        }
        SpanBuilder parent = spanStack.peek();
        if (parent != null) {
            parent.children.add(span);
        } else {
            completedSpans.add(span);
        }
    }

    @Override
    public long getNanoAnchor() {
        return nanoAnchor;
    }

    @Override
    @Nullable
    public SearchTraceResult.ShardTraceResult buildResult() {
        if (false == anchorSet) {
            return null;
        }

        // Close any remaining open spans
        long offsetNanos = System.nanoTime() - nanoAnchor;
        while (false == spanStack.isEmpty()) {
            SpanBuilder unclosed = spanStack.pop();
            unclosed.stopOffsetNanos = offsetNanos;
            TraceSpan completed = unclosed.build();
            SpanBuilder parent = spanStack.peek();
            if (parent != null) {
                parent.children.add(completed);
            } else {
                completedSpans.add(completed);
            }
        }

        if (completedSpans.isEmpty()) {
            return null;
        }

        return new SearchTraceResult.ShardTraceResult(nodeId, nodeName, wallClockAnchorMillis, List.copyOf(completedSpans));
    }

    /**
     * Mutable builder for constructing a {@link TraceSpan} on the shard level.
     */
    private static final class SpanBuilder {
        final String name;
        long startOffsetNanos;
        long stopOffsetNanos;
        final Map<String, Object> details = new LinkedHashMap<>();
        final List<TraceSpan> children = new ArrayList<>();

        SpanBuilder(String name) {
            this.name = name;
        }

        TraceSpan build() {
            return new TraceSpan(name, null, null, startOffsetNanos, stopOffsetNanos, Map.copyOf(details), List.copyOf(children));
        }
    }
}
