/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SynonymSet implements Writeable, ToXContentObject {

    public static final ParseField SYNONYMS_FIELD = new ParseField("synonyms");
    private static final ConstructingObjectParser<SynonymSet, Void> PARSER = new ConstructingObjectParser<>("synonyms", args -> {
        @SuppressWarnings("unchecked")
        final List<String> synonyms = (List<String>) args[0];
        return new SynonymSet(synonyms.toArray(new String[0]));
    });

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), SYNONYMS_FIELD);
    }

    private final String[] synonyms;

    public SynonymSet(String[] synonyms) {
        Objects.requireNonNull(synonyms, "synonyms cannot be null");
        this.synonyms = synonyms;

        if (Arrays.stream(synonyms).anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("synonym has an empty value");
        }
    }

    public SynonymSet(StreamInput in) throws IOException {
        this.synonyms = in.readStringArray();
    }

    public static SynonymSet fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static SynonymSet fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return SynonymSet.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SYNONYMS_FIELD.getPreferredName(), synonyms);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(synonyms);
    }
}
