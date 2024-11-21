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

package co.elastic.elasticsearch.metering.usagereports.publisher;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A usage record to send to the billing service.
 * <a href="https://github.com/elastic/usage-api/blob/main/api/user-v1-spec.yml">Swagger spec</a>,
 * <a href="https://github.com/elastic/mx-team/blob/main/teams/billing/services/usage_record_schema_v2.md">
 *     full descriptive schema (Level 1 only)</a>
 */
public record UsageRecord(String id, Instant usageTimestamp, UsageMetrics usage, UsageSource source) implements ToXContentObject {

    private static final String ID = "id";
    private static final String USAGE_TIMESTAMP = "usage_timestamp";
    private static final String USAGE = "usage";
    private static final String SOURCE = "source";

    private static final ConstructingObjectParser<UsageRecord, Void> PARSER = new ConstructingObjectParser<>(
        "usage_record",
        true,
        args -> new UsageRecord((String) args[0], Instant.parse((String) args[1]), (UsageMetrics) args[2], (UsageSource) args[3])
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField(ID));
        PARSER.declareString(constructorArg(), new ParseField(USAGE_TIMESTAMP));
        PARSER.declareObject(constructorArg(), (p, c) -> UsageMetrics.fromXContent(p), new ParseField(USAGE));
        PARSER.declareObject(constructorArg(), (p, c) -> UsageSource.fromXContent(p), new ParseField(SOURCE));
    }
    public UsageRecord {
        Objects.requireNonNull(id);
        Objects.requireNonNull(usageTimestamp);
        Objects.requireNonNull(usage);
        Objects.requireNonNull(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID, id);
        builder.field(USAGE_TIMESTAMP, usageTimestamp.toString());
        builder.field("creation_timestamp", params.param("creation_timestamp"));
        builder.field(USAGE, usage);
        builder.field(SOURCE, source);
        builder.endObject();
        return builder;
    }

    // Public visibility only for tests
    public static UsageRecord fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
