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

package co.elastic.elasticsearch.serverless.security.cloud;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record CloudCredentialsMetadata(String type, boolean internal, Instant creation, @Nullable Instant expiration) {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField INTERNAL_FIELD = new ParseField("internal");
    private static final ParseField CREATION_FIELD = new ParseField("creation");
    private static final ParseField EXPIRATION_FIELD = new ParseField("expiration");

    private static final ConstructingObjectParser<CloudCredentialsMetadata, Void> PARSER = buildParser();

    private static ConstructingObjectParser<CloudCredentialsMetadata, Void> buildParser() {
        final ConstructingObjectParser<CloudCredentialsMetadata, Void> parser = new ConstructingObjectParser<>(
            "credentials",
            true,
            a -> new CloudCredentialsMetadata(
                (String) a[0],  // type
                (boolean) a[1], // internal
                Instant.ofEpochMilli((Long) a[2]), // creation
                a[3] != null ? Instant.ofEpochMilli((Long) a[3]) : null // expiration
            )
        );
        parser.declareString(constructorArg(), TYPE_FIELD);
        parser.declareBoolean(constructorArg(), INTERNAL_FIELD);
        parser.declareLong(constructorArg(), CREATION_FIELD);
        parser.declareLong(optionalConstructorArg(), EXPIRATION_FIELD);
        return parser;
    }

    public static CloudCredentialsMetadata parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
