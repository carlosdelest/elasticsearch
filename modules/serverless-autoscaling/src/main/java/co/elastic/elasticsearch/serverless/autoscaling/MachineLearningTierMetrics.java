/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.serverless.autoscaling;

import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record MachineLearningTierMetrics(
    int nodes,
    long nodeMemoryInBytes,
    long modelMemoryInBytes,
    int minNodes,
    long extraSingleNodeModelMemoryInBytes,
    int extraSingleNodeProcessors,
    long extraModelMemoryInBytes,
    int extraProcessors,
    long removeNodeMemoryInBytes,

    MetricQuality metricQuality
) implements AutoscalingMetrics {
    public MachineLearningTierMetrics(StreamInput in) throws IOException {
        this(
            in.readVInt(), // nodes
            in.readVLong(),  // nodeMemoryInBytes
            in.readVLong(), // modelMemoryInBytes
            in.readVInt(), // minNodes
            in.readVLong(), // extraSingleNodeModelMemoryInBytes
            in.readVInt(), // extraSingleNodeProcessors
            in.readVLong(), // extraModelMemoryInBytes
            in.readVInt(), // extraProcessors
            in.readVLong(), // removeNodeMemoryInBytes
            MetricQuality.readFrom(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(nodes);
        out.writeVLong(nodeMemoryInBytes);
        out.writeVLong(modelMemoryInBytes);
        out.writeVInt(minNodes);
        out.writeVLong(extraSingleNodeModelMemoryInBytes);
        out.writeVInt(extraSingleNodeProcessors);
        out.writeVLong(extraModelMemoryInBytes);
        out.writeVInt(extraProcessors);
        out.writeVLong(removeNodeMemoryInBytes);
        metricQuality.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.object("metrics", (objectBuilder) -> {
            serializeMetric(builder, "nodes", nodes, metricQuality);
            serializeMetric(builder, "node_memory_in_bytes", nodeMemoryInBytes, metricQuality);
            serializeMetric(builder, "model_memory_in_bytes", modelMemoryInBytes, metricQuality);
            serializeMetric(builder, "min_nodes", minNodes, metricQuality);
            serializeMetric(builder, "extra_single_node_model_memory_in_bytes", extraSingleNodeModelMemoryInBytes, metricQuality);
            serializeMetric(builder, "extra_single_node_processors", extraSingleNodeProcessors, metricQuality);
            serializeMetric(builder, "extra_model_memory_in_bytes", extraModelMemoryInBytes, metricQuality);
            serializeMetric(builder, "extra_processors", extraProcessors, metricQuality);
            serializeMetric(builder, "remove_node_memory_in_bytes", removeNodeMemoryInBytes, metricQuality);
        });
        builder.endObject();
        return builder;
    }
}
