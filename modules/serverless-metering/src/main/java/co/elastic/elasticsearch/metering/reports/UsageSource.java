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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record UsageSource(String id, String instanceGroupId, @Nullable Map<String, String> metadata) implements ToXContentObject {

    private static final String ID = "id";
    private static final String INSTANCE_GROUP_ID = "instance_group_id";
    private static final String METADATA = "metadata";
    private static final ConstructingObjectParser<UsageSource, Void> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>("usage_source", true, a -> {
            String id = (String) a[0];
            String instanceGroupId = (String) a[1];
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>) a[2];
            return new UsageSource(id, instanceGroupId, metadata);
        });
        PARSER.declareString(constructorArg(), new ParseField(ID));
        PARSER.declareString(constructorArg(), new ParseField(INSTANCE_GROUP_ID));
        PARSER.declareObject(constructorArg(), (parser, s) -> parser.map(), new ParseField(METADATA));
    }

    public UsageSource {
        Objects.requireNonNull(id);
        assert id.startsWith("es-");
        Objects.requireNonNull(instanceGroupId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID, id);
        builder.field(INSTANCE_GROUP_ID, instanceGroupId);
        if (metadata != null) {
            builder.startObject(METADATA).mapContents(metadata).endObject();
        }
        builder.endObject();
        return builder;
    }

    public static UsageSource fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
