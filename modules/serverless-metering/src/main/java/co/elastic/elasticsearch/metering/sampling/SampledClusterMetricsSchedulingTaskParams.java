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

package co.elastic.elasticsearch.metering.sampling;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Encapsulates the parameters needed to start the {@link SampledClusterMetricsSchedulingTask} task. Currently, no parameters are required.
 */
public class SampledClusterMetricsSchedulingTaskParams implements PersistentTaskParams {

    public static final SampledClusterMetricsSchedulingTaskParams INSTANCE = new SampledClusterMetricsSchedulingTaskParams();

    public static final ObjectParser<SampledClusterMetricsSchedulingTaskParams, Void> PARSER = new ObjectParser<>(
        SampledClusterMetricsSchedulingTask.TASK_NAME,
        true,
        () -> INSTANCE
    );

    SampledClusterMetricsSchedulingTaskParams() {}

    SampledClusterMetricsSchedulingTaskParams(StreamInput ignored) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return SampledClusterMetricsSchedulingTask.TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) {}

    public static SampledClusterMetricsSchedulingTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SampledClusterMetricsSchedulingTaskParams;
    }
}
