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
import java.util.List;
import java.util.Objects;

/**
 * Container for query trace results to be included in the response.
 * Contains the trace ID, root span(s), and total duration.
 *
 * <p>This is the top-level object that gets serialized into the "trace" field
 * of both ES|QL and Query DSL responses.
 *
 * <p>Example JSON output:
 * <pre>{@code
 * {
 *   "trace": {
 *     "trace_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
 *     "total_duration_nanos": 150000000,
 *     "spans": [
 *       {
 *         "span_id": "a1b2c3d4e5f6a7b8",
 *         "operation_name": "esql.query",
 *         "start_time_nanos": 1704067200000000000,
 *         "end_time_nanos": 1704067200150000000,
 *         "duration_nanos": 150000000,
 *         "attributes": {...},
 *         "children": [...]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 */
public class QueryTraceResults implements Writeable, ToXContentObject {

    private final String traceId;
    private final List<QueryTraceSpan> spans;
    private final long totalDurationNanos;

    /**
     * Creates trace results with a single root span.
     *
     * @param traceId            the trace ID (32 hex characters)
     * @param rootSpan           the root span of the trace
     * @param totalDurationNanos the total duration in nanoseconds
     */
    public QueryTraceResults(String traceId, QueryTraceSpan rootSpan, long totalDurationNanos) {
        this(traceId, rootSpan != null ? List.of(rootSpan) : Collections.emptyList(), totalDurationNanos);
    }

    /**
     * Creates trace results with multiple root spans.
     *
     * @param traceId            the trace ID (32 hex characters)
     * @param spans              the list of root spans
     * @param totalDurationNanos the total duration in nanoseconds
     */
    public QueryTraceResults(String traceId, List<QueryTraceSpan> spans, long totalDurationNanos) {
        this.traceId = Objects.requireNonNull(traceId, "traceId must not be null");
        this.spans = new ArrayList<>(Objects.requireNonNull(spans, "spans must not be null"));
        this.totalDurationNanos = totalDurationNanos;
    }

    /**
     * Read from a stream.
     */
    public QueryTraceResults(StreamInput in) throws IOException {
        this.traceId = in.readString();
        this.spans = in.readCollectionAsList(QueryTraceSpan::new);
        this.totalDurationNanos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(traceId);
        out.writeCollection(spans);
        out.writeVLong(totalDurationNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("trace_id", traceId);
        builder.field("total_duration_nanos", totalDurationNanos);

        builder.startArray("spans");
        for (QueryTraceSpan span : spans) {
            span.toXContent(builder, params);
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

    /**
     * @return the trace ID (32 hex characters, W3C Trace Context format)
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * @return unmodifiable list of root spans
     */
    public List<QueryTraceSpan> getSpans() {
        return Collections.unmodifiableList(spans);
    }

    /**
     * @return the total duration in nanoseconds
     */
    public long getTotalDurationNanos() {
        return totalDurationNanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTraceResults that = (QueryTraceResults) o;
        return totalDurationNanos == that.totalDurationNanos
            && Objects.equals(traceId, that.traceId)
            && Objects.equals(spans, that.spans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceId, spans, totalDurationNanos);
    }

    @Override
    public String toString() {
        return "QueryTraceResults{" + "traceId='" + traceId + '\'' + ", spans=" + spans.size() + ", totalDuration=" + totalDurationNanos
            + "ns}";
    }
}
