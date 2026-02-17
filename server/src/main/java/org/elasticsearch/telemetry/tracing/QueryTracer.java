/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import org.elasticsearch.core.Releasable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Tracer implementation that collects trace spans locally for inclusion in query responses.
 * This is an alternative to APMTracer when request-level tracing is needed without exporting
 * to an APM/OpenTelemetry backend.
 *
 * <p>Usage pattern:
 * <pre>{@code
 * RequestTracer tracer = new RequestTracer();
 * tracer.startTrace(context, traceable, "esql.query", attributes);
 * try {
 *     // ... execute query phases, each calling startTrace/stopTrace ...
 * } finally {
 *     tracer.stopTrace(traceable);
 * }
 * QueryTraceResults results = tracer.getResults();
 * }</pre>
 *
 * <p>This tracer can be used in combination with APMTracer via a CompositeTracer pattern
 * if both request-level tracing and APM export are desired.
 *
 * @see Tracer
 * @see QueryTraceContext
 * @see QueryTraceResults
 */
public class QueryTracer implements Tracer {

    /**
     * A no-op tracer that does nothing. Use this when tracing is disabled to avoid null checks.
     */
    public static final QueryTracer NOOP = new NoopRequestTracer();

    private final QueryTraceContext traceContext;
    private final Map<String, String> traceableToSpanId;

    /**
     * Creates a new RequestTracer with an auto-generated trace ID.
     */
    public QueryTracer() {
        this.traceContext = new QueryTraceContext();
        this.traceableToSpanId = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new RequestTracer with the specified trace context.
     *
     * @param traceContext the trace context to use for collecting spans
     */
    public QueryTracer(QueryTraceContext traceContext) {
        this.traceContext = traceContext;
        this.traceableToSpanId = new ConcurrentHashMap<>();
    }

    /**
     * Creates a child RequestTracer from a parent trace context.
     * This is used on remote nodes to continue the trace under the parent span.
     *
     * @param parentContext the trace context from the parent node
     * @return a new RequestTracer that continues the trace, or NOOP if parent is disabled
     */
    public static QueryTracer fromParent(TraceParentContext parentContext) {
        if (parentContext == null || !parentContext.isEnabled()) {
            return NOOP;
        }
        return new QueryTracer(QueryTraceContext.fromParent(parentContext));
    }

    @Override
    public void startTrace(TraceContext traceContext, Traceable traceable, String name, Map<String, Object> attributes) {
        QueryTraceSpan span = this.traceContext.startSpan(name, attributes);
        traceableToSpanId.put(traceable.getSpanId(), span.getSpanId());
    }

    @Override
    public void startTrace(String name, Map<String, Object> attributes) {
        // Start a span without associating it with a Traceable
        // This creates an anonymous span that can only be stopped with stopTrace()
        this.traceContext.startSpan(name, attributes);
    }

    @Override
    public void stopTrace(Traceable traceable) {
        String spanId = traceableToSpanId.remove(traceable.getSpanId());
        if (spanId != null) {
            traceContext.endSpan(spanId);
        }
    }

    @Override
    public void stopTrace() {
        // End the current span on the stack
        traceContext.endCurrentSpan();
    }

    @Override
    public void addEvent(Traceable traceable, String eventName) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            // Events are recorded as attributes since QueryTraceSpan doesn't have a separate event list
            traceContext.setAttribute(spanId, "event." + eventName, System.nanoTime());
        }
    }

    @Override
    public void addError(Traceable traceable, Throwable throwable) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            traceContext.addError(spanId, throwable);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, boolean value) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            traceContext.setAttribute(spanId, key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, double value) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            traceContext.setAttribute(spanId, key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, long value) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            traceContext.setAttribute(spanId, key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, String value) {
        String spanId = traceableToSpanId.get(traceable.getSpanId());
        if (spanId != null) {
            traceContext.setAttribute(spanId, key, value);
        }
    }

    @Override
    public Releasable withScope(Traceable traceable) {
        // RequestTracer doesn't need scope management like APMTracer
        // since all spans are stored locally in the context
        return () -> {};
    }

    /**
     * Gets the trace results collected during query execution.
     * This should be called after all spans have been completed.
     *
     * @return the trace results containing all spans in a hierarchical tree structure
     */
    public QueryTraceResults getResults() {
        return traceContext.buildResults();
    }

    /**
     * @return the trace ID for this trace (32 hex characters)
     */
    public String getTraceId() {
        return traceContext.getTraceId();
    }

    /**
     * @return the root span ID, or null if no spans have been started
     */
    public String rootSpanId() {
        return traceContext.getRootSpanId();
    }

    /**
     * @return the root span as a Traceable, or null if no spans have been started
     */
    public Traceable rootSpan() {
        return traceContext.getRootSpan();
    }

    /**
     * @return the underlying trace context
     */
    public QueryTraceContext getTraceContext() {
        return traceContext;
    }

    /**
     * Starts a span directly on the context without requiring a Traceable.
     * This is a convenience method for instrumentation code that doesn't have
     * access to a Traceable object.
     *
     * @param operationName the name of the operation
     * @param attributes    initial attributes for the span
     * @return the QueryTraceSpan object (which implements Traceable)
     */
    public QueryTraceSpan startSpan(String operationName, Map<String, Object> attributes) {
        return traceContext.startSpan(operationName, attributes);
    }

    /**
     * Ends a span by its Traceable reference.
     *
     * @param traceable the traceable span to end
     */
    public void endSpan(Traceable traceable) {
        traceContext.endSpan(traceable);
    }

    /**
     * Ends a span directly by its ID.
     *
     * @param spanId the ID of the span to end
     */
    public void endSpan(String spanId) {
        traceContext.endSpan(spanId);
    }

    /**
     * Sets an attribute on the current active span.
     *
     * @param key   the attribute key
     * @param value the attribute value
     */
    public void setCurrentAttribute(String key, Object value) {
        traceContext.setCurrentAttribute(key, value);
    }

    /**
     * Records an error on the current active span.
     *
     * @param throwable the error that occurred
     */
    public void addCurrentError(Throwable throwable) {
        traceContext.addCurrentError(throwable);
    }

    /**
     * Gets the current trace parent context for propagating to remote nodes.
     * This should be called before sending a request to a remote node, and the
     * returned context should be included in the request.
     *
     * @return the trace parent context with current trace ID and active span ID
     */
    public TraceParentContext getTraceParentContext() {
        return traceContext.getTraceParentContext();
    }

    /**
     * Adds child spans from a remote node's trace results to the current active span.
     * This is called when a response is received from a remote node that includes trace results.
     *
     * @param childResults the trace results from the remote node, may be null
     */
    public void addChildTraceResults(QueryTraceResults childResults) {
        traceContext.addChildTraceResults(childResults);
    }

    /**
     * @return true if this tracer is actually collecting traces, false for NOOP
     */
    public boolean isEnabled() {
        return true;
    }

    /**
     * A no-op implementation of RequestTracer that does nothing.
     * All methods are no-ops and return dummy values where needed.
     */
    private static class NoopRequestTracer extends QueryTracer {
        private static final String NOOP_SPAN_ID = "";
        private static final QueryTraceSpan NOOP_SPAN = new QueryTraceSpan(NOOP_SPAN_ID, "noop", 0L);

        NoopRequestTracer() {
            super(new QueryTraceContext());
        }

        @Override
        public void startTrace(TraceContext traceContext, Traceable traceable, String name, Map<String, Object> attributes) {
            // no-op
        }

        @Override
        public void startTrace(String name, Map<String, Object> attributes) {
            // no-op
        }

        @Override
        public void stopTrace(Traceable traceable) {
            // no-op
        }

        @Override
        public void stopTrace() {
            // no-op
        }

        @Override
        public void addEvent(Traceable traceable, String eventName) {
            // no-op
        }

        @Override
        public void addError(Traceable traceable, Throwable throwable) {
            // no-op
        }

        @Override
        public void setAttribute(Traceable traceable, String key, boolean value) {
            // no-op
        }

        @Override
        public void setAttribute(Traceable traceable, String key, double value) {
            // no-op
        }

        @Override
        public void setAttribute(Traceable traceable, String key, long value) {
            // no-op
        }

        @Override
        public void setAttribute(Traceable traceable, String key, String value) {
            // no-op
        }

        @Override
        public QueryTraceResults getResults() {
            return null;
        }

        @Override
        public String getTraceId() {
            return null;
        }

        @Override
        public String rootSpanId() {
            return NOOP_SPAN_ID;
        }

        @Override
        public Traceable rootSpan() {
            return NOOP_SPAN;
        }

        @Override
        public QueryTraceSpan startSpan(String operationName, Map<String, Object> attributes) {
            return NOOP_SPAN;
        }

        @Override
        public void endSpan(Traceable traceable) {
            // no-op
        }

        @Override
        public void endSpan(String spanId) {
            // no-op
        }

        @Override
        public void setCurrentAttribute(String key, Object value) {
            // no-op
        }

        @Override
        public void addCurrentError(Throwable throwable) {
            // no-op
        }

        @Override
        public TraceParentContext getTraceParentContext() {
            return TraceParentContext.NONE;
        }

        @Override
        public void addChildTraceResults(QueryTraceResults childResults) {
            // no-op
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    }
}
