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

import co.elastic.elasticsearch.stateless.autoscaling.AbstractBaseTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;

import java.io.IOException;
import java.util.Objects;

public class MachineLearningTierMetrics extends AbstractBaseTierMetrics implements AutoscalingMetrics {

    private final MlAutoscalingStats autoscalingResources;

    public MachineLearningTierMetrics(MlAutoscalingStats autoscalingResources) {
        super();
        this.autoscalingResources = autoscalingResources;
    }

    public MachineLearningTierMetrics(String reason, ElasticsearchException exception) {
        super(reason, exception);
        this.autoscalingResources = null;
    }

    public MachineLearningTierMetrics(StreamInput in) throws IOException {
        super(in);
        this.autoscalingResources = in.readOptionalWriteable(MlAutoscalingStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(autoscalingResources);
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.object("metrics", (objectBuilder) -> {
            serializeMetric(builder, "nodes", autoscalingResources.nodes(), MetricQuality.EXACT);
            serializeMetric(builder, "node_memory_in_bytes", autoscalingResources.perNodeMemoryInBytes(), MetricQuality.EXACT);
            serializeMetric(builder, "model_memory_in_bytes", autoscalingResources.modelMemoryInBytesSum(), MetricQuality.EXACT);
            serializeMetric(builder, "min_nodes", autoscalingResources.minNodes(), MetricQuality.EXACT);
            serializeMetric(
                builder,
                "extra_single_node_model_memory_in_bytes",
                autoscalingResources.extraSingleNodeModelMemoryInBytes(),
                MetricQuality.EXACT
            );
            serializeMetric(builder, "extra_single_node_processors", autoscalingResources.extraSingleNodeProcessors(), MetricQuality.EXACT);
            serializeMetric(builder, "extra_model_memory_in_bytes", autoscalingResources.extraModelMemoryInBytes(), MetricQuality.EXACT);
            serializeMetric(builder, "extra_processors", autoscalingResources.extraProcessors(), MetricQuality.EXACT);
            serializeMetric(builder, "remove_node_memory_in_bytes", autoscalingResources.removeNodeMemoryInBytes(), MetricQuality.EXACT);
            serializeMetric(
                builder,
                "per_node_memory_overhead_in_bytes",
                autoscalingResources.perNodeMemoryOverheadInBytes(),
                MetricQuality.EXACT
            );
        });
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final MachineLearningTierMetrics that = (MachineLearningTierMetrics) other;

        return Objects.equals(this.autoscalingResources, that.autoscalingResources)
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(autoscalingResources, reason, exception);
    }
}
