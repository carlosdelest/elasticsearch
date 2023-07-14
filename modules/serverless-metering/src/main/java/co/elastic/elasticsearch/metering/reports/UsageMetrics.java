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

package co.elastic.elasticsearch.metering.reports;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public record UsageMetrics(
    String type,
    @Nullable String subType,
    long quantity,
    @Nullable TimeValue period,
    @Nullable String cause,
    @Nullable Map<String, ?> metadata
) implements ToXContentObject {
    public UsageMetrics {
        Objects.requireNonNull(type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        if (subType != null) {
            builder.field("sub_type", subType);
        }
        builder.field("quantity", quantity);
        if (period != null) {
            builder.field("period_seconds", period.seconds());
        }
        if (cause != null) {
            builder.field("cause", cause);
        }
        if (metadata != null) {
            builder.startObject("metadata").mapContents(metadata).endObject();
        }
        builder.endObject();
        return builder;
    }
}
