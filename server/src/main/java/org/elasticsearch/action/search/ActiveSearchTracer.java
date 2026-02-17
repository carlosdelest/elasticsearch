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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Active implementation of {@link SearchTracer} that captures real timing data on the
 * coordinator node. Uses a stack-based span model to build a nested span tree.
 *
 * <p>Thread safety: this class is NOT thread-safe. It is designed to be used from the
 * coordinator's search action thread. Shard results arrive asynchronously but are attached
 * via {@link #attachShardResult} which should be called from the coordinator thread
 * (inside phase callbacks that are already serialized by the search framework).
 */
public final class ActiveSearchTracer implements SearchTracer {

    private final String coordinatorNodeId;
    private final String coordinatorNodeName;
    private final long wallClockAnchorMillis;
    private final long nanoAnchor;

    /**
     * Node anchors collected from shard results. Keyed by node ID.
     */
    private final Map<String, SearchTraceResult.NodeAnchor> nodeAnchors;

    /**
     * The span stack. The bottom element is always the root "search" span.
     * Each entry is a mutable builder that accumulates children until stopped.
     */
    private final Deque<SpanBuilder> spanStack;

    /**
     * The completed root span, set when the root "search" phase is stopped or
     * when {@link #buildResult()} finalizes the trace.
     */
    private TraceSpan completedRoot;

    public ActiveSearchTracer(String coordinatorNodeId, String coordinatorNodeName) {
        this.coordinatorNodeId = coordinatorNodeId;
        this.coordinatorNodeName = coordinatorNodeName;
        this.wallClockAnchorMillis = System.currentTimeMillis();
        this.nanoAnchor = System.nanoTime();
        this.nodeAnchors = new LinkedHashMap<>();
        this.spanStack = new ArrayDeque<>();

        // Register coordinator node anchor
        nodeAnchors.put(coordinatorNodeId, new SearchTraceResult.NodeAnchor(coordinatorNodeName, wallClockAnchorMillis));

        // Push root "search" span
        SpanBuilder root = new SpanBuilder("search", coordinatorNodeId, null);
        root.startOffsetNanos = 0;
        spanStack.push(root);
    }

    @Override
    public void startPhase(String name) {
        long offsetNanos = System.nanoTime() - nanoAnchor;
        SpanBuilder span = new SpanBuilder(name, coordinatorNodeId, null);
        span.startOffsetNanos = offsetNanos;
        spanStack.push(span);
    }

    @Override
    public void stopPhase(String name) {
        long offsetNanos = System.nanoTime() - nanoAnchor;
        SpanBuilder top = spanStack.peek();
        if (top == null || false == top.name.equals(name)) {
            // Defensive: if the stack is misaligned, don't crash — just skip.
            // This can happen if a phase fails before stopPhase is called.
            return;
        }
        spanStack.pop();
        top.stopOffsetNanos = offsetNanos;
        TraceSpan completed = top.build();

        SpanBuilder parent = spanStack.peek();
        if (parent != null) {
            parent.children.add(completed);
        } else {
            // This was the root span
            completedRoot = completed;
        }
    }

    @Override
    public void attachShardResult(String shard, SearchTraceResult.ShardTraceResult result) {
        if (result == null) {
            return;
        }
        // Register the shard's node anchor if not already present
        nodeAnchors.putIfAbsent(result.getNodeId(), new SearchTraceResult.NodeAnchor(result.getNodeName(), result.getWallClockAnchorMillis()));

        // Wrap shard spans as children of the current phase
        SpanBuilder currentPhase = spanStack.peek();
        if (currentPhase != null) {
            for (TraceSpan shardSpan : result.getSpans()) {
                currentPhase.children.add(shardSpan);
            }
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
    @Nullable
    public SearchTraceResult buildResult() {
        // Finalize the root span if it hasn't been stopped yet
        if (completedRoot == null && false == spanStack.isEmpty()) {
            long offsetNanos = System.nanoTime() - nanoAnchor;
            // Close any remaining open spans (shouldn't happen in normal flow)
            while (spanStack.size() > 1) {
                SpanBuilder unclosed = spanStack.pop();
                unclosed.stopOffsetNanos = offsetNanos;
                SpanBuilder parent = spanStack.peek();
                if (parent != null) {
                    parent.children.add(unclosed.build());
                }
            }
            // Close the root span
            SpanBuilder root = spanStack.pop();
            root.stopOffsetNanos = offsetNanos;
            completedRoot = root.build();
        }

        if (completedRoot == null) {
            return null;
        }

        return new SearchTraceResult(Map.copyOf(nodeAnchors), completedRoot);
    }

    @Override
    public ShardSearchTracer shardTracer() {
        return new ActiveShardSearchTracer();
    }

    /**
     * Mutable builder for constructing a {@link TraceSpan}. Used internally during
     * the stack-based span construction.
     */
    private static final class SpanBuilder {
        final String name;
        final String nodeId;
        final String shard;
        long startOffsetNanos;
        long stopOffsetNanos;
        final Map<String, Object> details = new LinkedHashMap<>();
        final List<TraceSpan> children = new ArrayList<>();

        SpanBuilder(String name, String nodeId, String shard) {
            this.name = name;
            this.nodeId = nodeId;
            this.shard = shard;
        }

        TraceSpan build() {
            return new TraceSpan(name, nodeId, shard, startOffsetNanos, stopOffsetNanos, Map.copyOf(details), List.copyOf(children));
        }
    }
}
