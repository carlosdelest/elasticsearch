/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the trace context that needs to be propagated across nodes.
 * This is sent with requests to remote nodes so they can create child spans
 * under the correct parent.
 *
 * <p>This follows the W3C Trace Context specification for trace propagation.
 *
 * @see <a href="https://www.w3.org/TR/trace-context/">W3C Trace Context</a>
 */
public class TraceParentContext implements Writeable {

    /**
     * Represents "no tracing" - use this instead of null when tracing is disabled.
     */
    public static final TraceParentContext NONE = new TraceParentContext(null, null);

    @Nullable
    private final String traceId;
    @Nullable
    private final String parentSpanId;

    /**
     * Creates a new TraceParentContext.
     *
     * @param traceId the trace ID (32 hex characters), or null if tracing is disabled
     * @param parentSpanId the parent span ID (16 hex characters), or null if this is a root span
     */
    public TraceParentContext(@Nullable String traceId, @Nullable String parentSpanId) {
        this.traceId = traceId;
        this.parentSpanId = parentSpanId;
    }

    /**
     * Creates a TraceParentContext from a stream.
     */
    public TraceParentContext(StreamInput in) throws IOException {
        this.traceId = in.readOptionalString();
        this.parentSpanId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(traceId);
        out.writeOptionalString(parentSpanId);
    }

    /**
     * @return the trace ID, or null if tracing is disabled
     */
    @Nullable
    public String traceId() {
        return traceId;
    }

    /**
     * @return the parent span ID, or null if this is a root span or tracing is disabled
     */
    @Nullable
    public String parentSpanId() {
        return parentSpanId;
    }

    /**
     * @return true if tracing is enabled (trace ID is present)
     */
    public boolean isEnabled() {
        return traceId != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceParentContext that = (TraceParentContext) o;
        return Objects.equals(traceId, that.traceId) && Objects.equals(parentSpanId, that.parentSpanId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceId, parentSpanId);
    }

    @Override
    public String toString() {
        if (traceId == null) {
            return "TraceParentContext{disabled}";
        }
        return "TraceParentContext{traceId='" + traceId + "', parentSpanId='" + parentSpanId + "'}";
    }
}
