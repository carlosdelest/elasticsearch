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
 * The complete result of a search trace, containing node timing anchors and a span tree.
 * This is the top-level object attached to {@link SearchResponse} when tracing is enabled.
 *
 * <p>The output is designed for Gantt chart rendering. Each node has a wall-clock anchor
 * (milliseconds) paired with a nano anchor. All spans on that node use nanosecond offsets
 * from the nano anchor. The Gantt consumer aligns nodes using their wall-clock anchors
 * and positions each span at {@code [anchorMillis + startOffsetNanos/1e6, anchorMillis + (startOffsetNanos + durationNanos)/1e6]}.
 */
public final class SearchTraceResult implements Writeable, ToXContentObject {

    static final ParseField NODES_FIELD = new ParseField("nodes");
    static final ParseField SPANS_FIELD = new ParseField("spans");

    private final Map<String, NodeAnchor> nodes;
    private final TraceSpan rootSpan;

    public SearchTraceResult(Map<String, NodeAnchor> nodes, TraceSpan rootSpan) {
        this.nodes = Objects.requireNonNull(nodes, "nodes must not be null");
        this.rootSpan = Objects.requireNonNull(rootSpan, "rootSpan must not be null");
    }

    /**
     * Read from a stream.
     */
    public SearchTraceResult(StreamInput in) throws IOException {
        this.nodes = in.readMap(NodeAnchor::new);
        this.rootSpan = new TraceSpan(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(nodes, StreamOutput::writeWriteable);
        rootSpan.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NODES_FIELD.getPreferredName());
        for (Map.Entry<String, NodeAnchor> entry : nodes.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.field(SPANS_FIELD.getPreferredName());
        rootSpan.toXContent(builder, params);
        return builder.endObject();
    }

    public Map<String, NodeAnchor> getNodes() {
        return nodes;
    }

    public TraceSpan getRootSpan() {
        return rootSpan;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchTraceResult that = (SearchTraceResult) o;
        return Objects.equals(nodes, that.nodes) && Objects.equals(rootSpan, that.rootSpan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes, rootSpan);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * A timing anchor for a single node. Pairs a wall-clock timestamp (for cross-node alignment)
     * with metadata about the node.
     */
    public static final class NodeAnchor implements Writeable, ToXContentObject {

        static final ParseField NODE_NAME_FIELD = new ParseField("node_name");
        static final ParseField WALL_CLOCK_ANCHOR_MILLIS_FIELD = new ParseField("wall_clock_anchor_millis");

        private final String nodeName;
        private final long wallClockAnchorMillis;

        public NodeAnchor(String nodeName, long wallClockAnchorMillis) {
            this.nodeName = Objects.requireNonNull(nodeName, "nodeName must not be null");
            this.wallClockAnchorMillis = wallClockAnchorMillis;
        }

        public NodeAnchor(StreamInput in) throws IOException {
            this.nodeName = in.readString();
            this.wallClockAnchorMillis = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeName);
            out.writeVLong(wallClockAnchorMillis);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NODE_NAME_FIELD.getPreferredName(), nodeName);
            builder.field(WALL_CLOCK_ANCHOR_MILLIS_FIELD.getPreferredName(), wallClockAnchorMillis);
            return builder.endObject();
        }

        public String getNodeName() {
            return nodeName;
        }

        public long getWallClockAnchorMillis() {
            return wallClockAnchorMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeAnchor that = (NodeAnchor) o;
            return wallClockAnchorMillis == that.wallClockAnchorMillis && Objects.equals(nodeName, that.nodeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeName, wallClockAnchorMillis);
        }

        @Override
        public String toString() {
            return "NodeAnchor{nodeName=" + nodeName + ", wallClockAnchorMillis=" + wallClockAnchorMillis + "}";
        }
    }

    /**
     * The trace result from a single shard, produced on the data node and transported back
     * to the coordinator. Contains the shard's node anchor and a list of spans representing
     * the shard-level execution phases (e.g. async_rewrite, lucene_rewrite, lucene_query).
     */
    public static final class ShardTraceResult implements Writeable {

        private final String nodeId;
        private final String nodeName;
        private final long wallClockAnchorMillis;
        private final List<TraceSpan> spans;

        public ShardTraceResult(String nodeId, String nodeName, long wallClockAnchorMillis, List<TraceSpan> spans) {
            this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
            this.nodeName = Objects.requireNonNull(nodeName, "nodeName must not be null");
            this.wallClockAnchorMillis = wallClockAnchorMillis;
            this.spans = spans == null ? List.of() : spans;
        }

        public ShardTraceResult(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.nodeName = in.readString();
            this.wallClockAnchorMillis = in.readVLong();
            this.spans = in.readCollectionAsList(TraceSpan::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(nodeName);
            out.writeVLong(wallClockAnchorMillis);
            out.writeCollection(spans);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getNodeName() {
            return nodeName;
        }

        public long getWallClockAnchorMillis() {
            return wallClockAnchorMillis;
        }

        public List<TraceSpan> getSpans() {
            return spans;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardTraceResult that = (ShardTraceResult) o;
            return wallClockAnchorMillis == that.wallClockAnchorMillis
                && Objects.equals(nodeId, that.nodeId)
                && Objects.equals(nodeName, that.nodeName)
                && Objects.equals(spans, that.spans);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, nodeName, wallClockAnchorMillis, spans);
        }

        @Override
        public String toString() {
            return "ShardTraceResult{nodeId=" + nodeId + ", nodeName=" + nodeName + ", spans=" + spans.size() + "}";
        }
    }
}
