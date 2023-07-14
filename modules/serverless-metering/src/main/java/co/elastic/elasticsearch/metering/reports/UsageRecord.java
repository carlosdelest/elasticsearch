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

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * A usage record to send to the billing service.
 * <a href="https://github.com/elastic/usage-api/blob/main/api/user-v1-spec.yml">Swagger spec</a>,
 * <a href="https://github.com/elastic/mx-team/blob/main/teams/billing/services/usage_record_schema_v2.md">
 *     full descriptive schema (Level 1 only)</a>
 */
public record UsageRecord(String id, Instant usageTimestamp, UsageMetrics usage, UsageSource source) implements ToXContentObject {
    public UsageRecord {
        Objects.requireNonNull(id);
        Objects.requireNonNull(usageTimestamp);
        Objects.requireNonNull(usage);
        Objects.requireNonNull(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("usage_timestamp", usageTimestamp.toString());
        builder.field("creation_timestamp", params.param("creation_timestamp"));
        builder.field("usage", usage);
        builder.field("source", source);
        builder.endObject();
        return builder;
    }
}
