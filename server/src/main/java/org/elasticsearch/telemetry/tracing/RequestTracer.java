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
public class RequestTracer implements Tracer {

    /**
     * A no-op tracer that does nothing. Use this when tracing is disabled to avoid null checks.
     */
    public static final RequestTracer NOOP = new NoopRequestTracer();

    private final QueryTraceContext traceContext;
    private final Map<String, String> traceableToSpanId;

    /**
     * Creates a new RequestTracer with an auto-generated trace ID.
     */
    public RequestTracer() {
        this.traceContext = new QueryTraceContext();
        this.traceableToSpanId = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new RequestTracer with the specified trace context.
     *
     * @param traceContext the trace context to use for collecting spans
     */
    public RequestTracer(QueryTraceContext traceContext) {
        this.traceContext = traceContext;
        this.traceableToSpanId = new ConcurrentHashMap<>();
    }

    @Override
    public void startTrace(TraceContext traceContext, Traceable traceable, String name, Map<String, Object> attributes) {
        String spanId = this.traceContext.startSpan(name, attributes);
        traceableToSpanId.put(traceable.getSpanId(), spanId);
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
     * @return the span ID for the newly created span
     */
    public String startSpan(String operationName, Map<String, Object> attributes) {
        return traceContext.startSpan(operationName, attributes);
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
     * @return true if this tracer is actually collecting traces, false for NOOP
     */
    public boolean isEnabled() {
        return true;
    }

    /**
     * A no-op implementation of RequestTracer that does nothing.
     * All methods are no-ops and return dummy values where needed.
     */
    private static class NoopRequestTracer extends RequestTracer {
        private static final String NOOP_SPAN_ID = "";

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
        public String startSpan(String operationName, Map<String, Object> attributes) {
            return NOOP_SPAN_ID;
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
        public boolean isEnabled() {
            return false;
        }
    }
}
