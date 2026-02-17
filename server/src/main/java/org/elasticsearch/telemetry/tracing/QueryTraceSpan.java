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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single trace span with timing information, attributes, and nested child spans.
 * This is used for request-level query tracing to capture timing data for different phases
 * of query execution.
 *
 * <p>The span follows OpenTelemetry-compatible structure with:
 * <ul>
 *   <li>W3C Trace Context format for IDs (span_id: 16 hex chars)</li>
 *   <li>Nested parent-child span relationships via the children list</li>
 *   <li>Rich attributes for contextual information</li>
 * </ul>
 */
public class QueryTraceSpan implements Traceable, Writeable, ToXContentObject {

    private final String spanId;
    private final String operationName;
    private final long startTimeNanos;
    private long endTimeNanos;
    private final Map<String, Object> attributes;
    private final List<QueryTraceSpan> children;
    private Throwable error;

    /**
     * Creates a new QueryTraceSpan.
     *
     * @param spanId         unique 16 hex character identifier for this span
     * @param operationName  the name of the operation (e.g., "esql.parse", "search.query_phase")
     * @param startTimeNanos the start time in nanoseconds since epoch
     */
    public QueryTraceSpan(String spanId, String operationName, long startTimeNanos) {
        this(spanId, operationName, startTimeNanos, 0, new HashMap<>(), new ArrayList<>());
    }

    /**
     * Full constructor for deserialization.
     */
    public QueryTraceSpan(
        String spanId,
        String operationName,
        long startTimeNanos,
        long endTimeNanos,
        Map<String, Object> attributes,
        List<QueryTraceSpan> children
    ) {
        this.spanId = Objects.requireNonNull(spanId, "spanId must not be null");
        this.operationName = Objects.requireNonNull(operationName, "operationName must not be null");
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.attributes = new HashMap<>(Objects.requireNonNull(attributes, "attributes must not be null"));
        this.children = new ArrayList<>(Objects.requireNonNull(children, "children must not be null"));
    }

    /**
     * Read from a stream.
     */
    public QueryTraceSpan(StreamInput in) throws IOException {
        this.spanId = in.readString();
        this.operationName = in.readString();
        this.startTimeNanos = in.readVLong();
        this.endTimeNanos = in.readVLong();
        this.attributes = in.readGenericMap();
        this.children = in.readCollectionAsList(QueryTraceSpan::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(spanId);
        out.writeString(operationName);
        out.writeVLong(startTimeNanos);
        out.writeVLong(endTimeNanos);
        out.writeGenericMap(attributes);
        out.writeCollection(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("span_id", spanId);
        builder.field("operation_name", operationName);
        builder.field("start_time_nanos", startTimeNanos);
        builder.field("end_time_nanos", endTimeNanos);
        builder.field("duration_nanos", getDurationNanos());

        if (attributes.isEmpty() == false) {
            builder.startObject("attributes");
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        } else {
            builder.startObject("attributes").endObject();
        }

        builder.startArray("children");
        for (QueryTraceSpan child : children) {
            child.toXContent(builder, params);
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

    /**
     * Marks the end of this span.
     *
     * @param endTimeNanos the end time in nanoseconds since epoch
     */
    public void end(long endTimeNanos) {
        this.endTimeNanos = endTimeNanos;
    }

    /**
     * Adds a child span to this span.
     *
     * @param child the child span to add
     */
    public void addChild(QueryTraceSpan child) {
        children.add(child);
    }

    /**
     * Sets an attribute on this span.
     *
     * @param key   the attribute key
     * @param value the attribute value (must be String, Long, Double, or Boolean)
     */
    public void setAttribute(String key, Object value) {
        if (value != null) {
            attributes.put(key, value);
        }
    }

    /**
     * Records an error that occurred during this span.
     *
     * @param throwable the error that occurred
     */
    public void setError(Throwable throwable) {
        this.error = throwable;
        if (throwable != null) {
            attributes.put("error", true);
            attributes.put("error.message", throwable.getMessage());
            attributes.put("error.type", throwable.getClass().getName());
        }
    }

    /**
     * @return the unique span identifier (16 hex characters)
     */
    @Override
    public String getSpanId() {
        return spanId;
    }

    /**
     * @return the operation name (e.g., "esql.parse", "search.query_phase")
     */
    public String getOperationName() {
        return operationName;
    }

    /**
     * @return the start time in nanoseconds since epoch
     */
    public long getStartTimeNanos() {
        return startTimeNanos;
    }

    /**
     * @return the end time in nanoseconds since epoch
     */
    public long getEndTimeNanos() {
        return endTimeNanos;
    }

    /**
     * @return the duration in nanoseconds
     */
    public long getDurationNanos() {
        return endTimeNanos - startTimeNanos;
    }

    /**
     * @return unmodifiable view of the attributes map
     */
    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * @return unmodifiable view of the children list
     */
    public List<QueryTraceSpan> getChildren() {
        return Collections.unmodifiableList(children);
    }

    /**
     * @return the error that occurred during this span, or null
     */
    public Throwable getError() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTraceSpan that = (QueryTraceSpan) o;
        return startTimeNanos == that.startTimeNanos
            && endTimeNanos == that.endTimeNanos
            && Objects.equals(spanId, that.spanId)
            && Objects.equals(operationName, that.operationName)
            && Objects.equals(attributes, that.attributes)
            && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spanId, operationName, startTimeNanos, endTimeNanos, attributes, children);
    }

    @Override
    public String toString() {
        return "QueryTraceSpan{"
            + "spanId='"
            + spanId
            + '\''
            + ", operationName='"
            + operationName
            + '\''
            + ", duration="
            + getDurationNanos()
            + "ns"
            + ", children="
            + children.size()
            + '}';
    }
}
