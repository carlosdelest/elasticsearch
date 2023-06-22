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

package co.elastic.elasticsearch.serverless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public record TierMetrics(Map<String, Metric> metrics) implements ToXContentObject, Writeable {

    public TierMetrics(Map<String, Metric> metrics) {
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    public TierMetrics(StreamInput input) throws IOException {
        this(input.readImmutableMap(StreamInput::readString, Metric::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(metrics, StreamOutput::writeString, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("metrics", metrics);
        builder.endObject();
        return builder;
    }
}
