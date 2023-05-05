/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SynonymSetListItem implements Writeable, ToXContentObject {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField COUNT_FIELD = new ParseField("count");
    private static final ConstructingObjectParser<SynonymSetListItem, Void> PARSER = new ConstructingObjectParser<>(
        "synonym_list_item",
        args -> {
            return new SynonymSetListItem((String) args[0], (Long) args[1]);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT_FIELD);
    }

    private final String name;
    private final Long count;

    public SynonymSetListItem(String name, Long count) {
        Objects.requireNonNull(name, "name cannot be null");
        this.name = name;

        Objects.requireNonNull(count, "count cannot be null");
        this.count = count;
    }

    public SynonymSetListItem(StreamInput in) throws IOException {
        this.name = in.readString();
        this.count = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.field(COUNT_FIELD.getPreferredName(), count);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(count);
    }
}
