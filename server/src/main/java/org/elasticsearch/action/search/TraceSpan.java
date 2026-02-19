/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A single span in a search trace tree. Each span represents a timed phase or sub-phase
 * of search execution, positioned relative to a per-node nano anchor. Spans form a tree
 * via the {@link #children} list, enabling Gantt chart rendering with proper nesting.
 *
 * <p>Coordinator-level spans (e.g. "query", "fetch") use the coordinator node's anchor.
 * Shard-level spans (e.g. "shard_query", "lucene_rewrite") use the data node's anchor.
 * The consuming Gantt chart tool aligns different nodes using their wall-clock anchors
 * from {@link SearchTraceResult.NodeAnchor}.
 */
public final class TraceSpan implements Writeable, ToXContentObject {

    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    static final ParseField SHARD_FIELD = new ParseField("shard");
    static final ParseField START_OFFSET_NANOS_FIELD = new ParseField("start_offset_nanos");
    static final ParseField DURATION_NANOS_FIELD = new ParseField("duration_nanos");
    static final ParseField DETAILS_FIELD = new ParseField("details");
    static final ParseField CHILDREN_FIELD = new ParseField("children");

    private final String name;
    private final String nodeId;
    private final String shard;
    private final long startOffsetNanos;
    private final long durationNanos;
    private final Map<String, Object> details;
    private final List<TraceSpan> children;

    /**
     * Creates a new trace span.
     *
     * @param name             the span name identifying the phase (e.g. "query", "fetch", "lucene_rewrite")
     * @param nodeId           the node where this span executed, or null for spans that inherit the parent's node
     * @param shard            the shard identifier (e.g. "[node2][idx][0]"), or null for non-shard spans
     * @param startOffsetNanos offset in nanoseconds from the per-node nano anchor when this span started
     * @param durationNanos    duration of this span in nanoseconds
     * @param details          additional key-value details (e.g. query text, slice count, docs fetched)
     * @param children         child spans nested under this span
     */
    public TraceSpan(
        String name,
        String nodeId,
        String shard,
        long startOffsetNanos,
        long durationNanos,
        Map<String, Object> details,
        List<TraceSpan> children
    ) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.nodeId = nodeId;
        this.shard = shard;
        this.startOffsetNanos = startOffsetNanos;
        this.durationNanos = durationNanos;
        this.details = details == null ? Map.of() : details;
        this.children = children == null ? List.of() : children;
    }

    /**
     * Read from a stream.
     */
    public TraceSpan(StreamInput in) throws IOException {
        this.name = in.readString();
        this.nodeId = in.readOptionalString();
        this.shard = in.readOptionalString();
        this.startOffsetNanos = in.readVLong();
        this.durationNanos = in.readVLong();
        this.details = in.readMap(StreamInput::readGenericValue);
        this.children = in.readCollectionAsList(TraceSpan::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(shard);
        out.writeVLong(startOffsetNanos);
        out.writeVLong(durationNanos);
        out.writeMap(details, StreamOutput::writeGenericValue);
        out.writeCollection(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        if (nodeId != null) {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
        }
        if (shard != null) {
            builder.field(SHARD_FIELD.getPreferredName(), shard);
        }
        builder.field(START_OFFSET_NANOS_FIELD.getPreferredName(), startOffsetNanos);
        builder.field(DURATION_NANOS_FIELD.getPreferredName(), durationNanos);
        if (false == details.isEmpty()) {
            builder.field(DETAILS_FIELD.getPreferredName(), details);
        }
        if (false == children.isEmpty()) {
            builder.startArray(CHILDREN_FIELD.getPreferredName());
            for (TraceSpan child : children) {
                child.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public String getName() {
        return name;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getShard() {
        return shard;
    }

    public long getStartOffsetNanos() {
        return startOffsetNanos;
    }

    public long getDurationNanos() {
        return durationNanos;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public List<TraceSpan> getChildren() {
        return children;
    }

    /**
     * Returns a copy of this span with the given node ID and shard, preserving all other fields.
     * Used by the coordinator to tag shard-level spans (which are built without node/shard info
     * on the data node) with their origin before inserting them into the coordinator span tree.
     */
    TraceSpan withNodeAndShard(String nodeId, String shard) {
        return new TraceSpan(name, nodeId, shard, startOffsetNanos, durationNanos, details, children);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceSpan that = (TraceSpan) o;
        return startOffsetNanos == that.startOffsetNanos
            && durationNanos == that.durationNanos
            && Objects.equals(name, that.name)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(shard, that.shard)
            && Objects.equals(details, that.details)
            && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, nodeId, shard, startOffsetNanos, durationNanos, details, children);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
