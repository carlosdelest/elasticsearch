/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects trace spans during query execution and maintains the span stack for
 * building a hierarchical tree structure. This class is thread-safe and can be
 * used in async execution contexts.
 *
 * <p>The context maintains:
 * <ul>
 *   <li>A unique trace ID (32 hex characters, W3C Trace Context format)</li>
 *   <li>A stack of active spans for establishing parent-child relationships</li>
 *   <li>A map of span IDs to spans for quick lookup</li>
 * </ul>
 *
 * <p>Usage pattern:
 * <pre>{@code
 * QueryTraceContext context = new QueryTraceContext();
 * String rootSpanId = context.startSpan("esql.query", attributes);
 * try {
 *     String childSpanId = context.startSpan("esql.parse", attributes);
 *     try {
 *         // ... do parsing work ...
 *     } finally {
 *         context.endSpan(childSpanId);
 *     }
 * } finally {
 *     context.endSpan(rootSpanId);
 * }
 * QueryTraceResults results = context.buildResults();
 * }</pre>
 */
public class QueryTraceContext {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private final String traceId;
    private final long startTimeNanos;
    private final Deque<QueryTraceSpan> spanStack;
    private final Map<String, QueryTraceSpan> spanMap;
    private QueryTraceSpan rootSpan;

    /**
     * Creates a new QueryTraceContext with an auto-generated trace ID.
     */
    public QueryTraceContext() {
        this(generateTraceId(), System.nanoTime());
    }

    /**
     * Creates a new QueryTraceContext with the specified trace ID.
     *
     * @param traceId the trace ID (32 hex characters)
     * @param startTimeNanos the start time in nanoseconds
     */
    public QueryTraceContext(String traceId, long startTimeNanos) {
        this.traceId = Objects.requireNonNull(traceId, "traceId must not be null");
        this.startTimeNanos = startTimeNanos;
        this.spanStack = new ArrayDeque<>();
        this.spanMap = new ConcurrentHashMap<>();
    }

    /**
     * Creates a child QueryTraceContext from a parent trace context.
     * This is used on remote nodes to continue the trace under the parent span.
     *
     * @param parentContext the trace context from the parent node
     * @return a new QueryTraceContext that continues the trace, or a new independent context if parent is null/disabled
     */
    public static QueryTraceContext fromParent(TraceParentContext parentContext) {
        if (parentContext == null || !parentContext.isEnabled()) {
            return new QueryTraceContext();
        }
        return new QueryTraceContext(parentContext.traceId(), System.nanoTime());
    }

    /**
     * Starts a new span with the given operation name and attributes.
     * If there is an active span on the stack, the new span becomes its child.
     *
     * @param operationName the name of the operation
     * @param attributes    initial attributes for the span
     * @return the span ID for the newly created span
     */
    public synchronized String startSpan(String operationName, Map<String, Object> attributes) {
        String spanId = generateSpanId();
        long now = System.nanoTime();

        QueryTraceSpan span = new QueryTraceSpan(spanId, operationName, now);

        // Add initial attributes
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                span.setAttribute(entry.getKey(), entry.getValue());
            }
        }

        // If there's a parent span, add this as a child
        QueryTraceSpan parent = spanStack.peek();
        if (parent != null) {
            parent.addChild(span);
        } else {
            // This is the root span
            rootSpan = span;
        }

        spanStack.push(span);
        spanMap.put(spanId, span);

        return spanId;
    }

    /**
     * Ends the span with the given ID.
     *
     * @param spanId the ID of the span to end
     * @throws IllegalStateException if the span is not found or not the current active span
     */
    public synchronized void endSpan(String spanId) {
        QueryTraceSpan span = spanMap.get(spanId);
        if (span == null) {
            throw new IllegalStateException("Span not found: " + spanId);
        }

        span.end(System.nanoTime());

        // Pop from stack - should be the top element
        QueryTraceSpan top = spanStack.peek();
        if (top != null && top.getSpanId().equals(spanId)) {
            spanStack.pop();
        }
    }

    /**
     * Ends the current active span (top of the stack).
     *
     * @throws IllegalStateException if there is no active span
     */
    public synchronized void endCurrentSpan() {
        QueryTraceSpan top = spanStack.peek();
        if (top == null) {
            throw new IllegalStateException("No active span to end");
        }
        endSpan(top.getSpanId());
    }

    /**
     * Sets an attribute on a specific span.
     *
     * @param spanId the ID of the span
     * @param key    the attribute key
     * @param value  the attribute value
     */
    public void setAttribute(String spanId, String key, Object value) {
        QueryTraceSpan span = spanMap.get(spanId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    /**
     * Sets an attribute on the current active span.
     *
     * @param key   the attribute key
     * @param value the attribute value
     */
    public synchronized void setCurrentAttribute(String key, Object value) {
        QueryTraceSpan current = spanStack.peek();
        if (current != null) {
            current.setAttribute(key, value);
        }
    }

    /**
     * Records an error on a specific span.
     *
     * @param spanId    the ID of the span
     * @param throwable the error that occurred
     */
    public void addError(String spanId, Throwable throwable) {
        QueryTraceSpan span = spanMap.get(spanId);
        if (span != null) {
            span.setError(throwable);
        }
    }

    /**
     * Records an error on the current active span.
     *
     * @param throwable the error that occurred
     */
    public synchronized void addCurrentError(Throwable throwable) {
        QueryTraceSpan current = spanStack.peek();
        if (current != null) {
            current.setError(throwable);
        }
    }

    /**
     * @return the current active span ID, or null if no span is active
     */
    public synchronized String getCurrentSpanId() {
        QueryTraceSpan current = spanStack.peek();
        return current != null ? current.getSpanId() : null;
    }

    /**
     * @return the trace ID (32 hex characters)
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * @return the root span ID, or null if no spans have been started
     */
    public String getRootSpanId() {
        return rootSpan != null ? rootSpan.getSpanId() : null;
    }

    /**
     * @return the start time in nanoseconds
     */
    public long getStartTimeNanos() {
        return startTimeNanos;
    }

    /**
     * Builds the final trace results.
     *
     * @return the trace results containing all spans in a tree structure
     */
    public QueryTraceResults buildResults() {
        long totalDuration = System.nanoTime() - startTimeNanos;
        return new QueryTraceResults(traceId, rootSpan, totalDuration);
    }

    /**
     * Gets the current trace parent context for propagating to remote nodes.
     * This should be called before sending a request to a remote node, and the
     * returned context should be included in the request.
     *
     * @return the trace parent context with current trace ID and active span ID
     */
    public synchronized TraceParentContext getTraceParentContext() {
        String currentSpanId = getCurrentSpanId();
        return new TraceParentContext(traceId, currentSpanId);
    }

    /**
     * Adds child spans from a remote node's trace results to the current active span.
     * This is called when a response is received from a remote node that includes trace results.
     *
     * @param childResults the trace results from the remote node, may be null
     */
    public synchronized void addChildTraceResults(QueryTraceResults childResults) {
        if (childResults == null || childResults.rootSpan() == null) {
            return;
        }

        QueryTraceSpan currentSpan = spanStack.peek();
        if (currentSpan != null) {
            // Add the root span from the child results as a child of the current span
            currentSpan.addChild(childResults.rootSpan());
        } else if (rootSpan != null) {
            // If no active span, add to root span
            rootSpan.addChild(childResults.rootSpan());
        }
    }

    /**
     * Adds a child span directly to the specified parent span.
     *
     * @param parentSpanId the ID of the parent span
     * @param childSpan the child span to add
     */
    public void addChildSpan(String parentSpanId, QueryTraceSpan childSpan) {
        if (parentSpanId == null || childSpan == null) {
            return;
        }
        QueryTraceSpan parent = spanMap.get(parentSpanId);
        if (parent != null) {
            parent.addChild(childSpan);
        }
    }

    /**
     * Generates a W3C Trace Context compatible trace ID (32 hex characters).
     *
     * @return a new trace ID
     */
    public static String generateTraceId() {
        byte[] bytes = new byte[16];
        SECURE_RANDOM.nextBytes(bytes);
        return HEX_FORMAT.formatHex(bytes);
    }

    /**
     * Generates a W3C Trace Context compatible span ID (16 hex characters).
     *
     * @return a new span ID
     */
    public static String generateSpanId() {
        byte[] bytes = new byte[8];
        SECURE_RANDOM.nextBytes(bytes);
        return HEX_FORMAT.formatHex(bytes);
    }
}
