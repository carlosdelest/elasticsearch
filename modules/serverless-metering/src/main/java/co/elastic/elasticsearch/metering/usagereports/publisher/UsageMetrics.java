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

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record UsageMetrics(
    // this is the metric value for a single metric & value, despite the name
    String type,
    @Nullable String subType,
    long quantity,
    @Nullable TimeValue period,
    @Nullable String cause,
    @Nullable Map<String, String> metadata
) implements ToXContentObject {

    private static final String TYPE = "type";
    private static final String SUBTYPE = "sub_type";
    private static final String QUANTITY = "quantity";
    private static final String PERIOD = "period_seconds";
    private static final String CAUSE = "cause";
    private static final String METADATA = "metadata";

    private static final ConstructingObjectParser<UsageMetrics, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>("usage_metrics", true, a -> {
            String type = (String) a[0];
            String subtype = (String) a[1];
            long quantity = (Long) a[2];
            long periodSeconds = (Long) a[3];
            TimeValue period = periodSeconds == Long.MIN_VALUE ? null : TimeValue.timeValueSeconds(periodSeconds);
            String cause = (String) a[4];
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>) a[5];
            return new UsageMetrics(type, subtype, quantity, period, cause, metadata);
        });
        PARSER.declareString(constructorArg(), new ParseField(TYPE));
        PARSER.declareString(optionalConstructorArg(), new ParseField(SUBTYPE));
        PARSER.declareLong(constructorArg(), new ParseField(QUANTITY));
        PARSER.declareLong(optionalConstructorArg(), new ParseField(PERIOD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(CAUSE));
        PARSER.declareObject(optionalConstructorArg(), (parser, s) -> parser.map(), new ParseField(METADATA));
    }

    public UsageMetrics {
        Objects.requireNonNull(type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE, type);
        if (subType != null) {
            builder.field(SUBTYPE, subType);
        }
        builder.field(QUANTITY, quantity);
        if (period != null) {
            builder.field(PERIOD, period.seconds());
        }
        if (cause != null) {
            builder.field(CAUSE, cause);
        }
        if (metadata != null) {
            builder.startObject(METADATA).mapContents(metadata).endObject();
        }
        builder.endObject();
        return builder;
    }

    static UsageMetrics fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
