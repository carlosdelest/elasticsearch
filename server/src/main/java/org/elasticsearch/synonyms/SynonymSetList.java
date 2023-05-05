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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SynonymSetList implements Writeable, ToXContentObject {

    public static final ParseField RESULT_FIELD = new ParseField("results");
    public static final ParseField COUNT_FIELD = new ParseField("count");

    private final List<SynonymSetListItem> itemsList;
    private final Long count;

    public SynonymSetList(StreamInput in) throws IOException {
        count = in.readLong();
        itemsList = in.readImmutableList(SynonymSetListItem::new);
    }

    public SynonymSetList(List<SynonymSetListItem> itemsList, Long count) {
        Objects.requireNonNull(itemsList, "list items must be specified");
        this.itemsList = itemsList;

        Objects.requireNonNull(count, "count must be specified");
        this.count = count;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeGenericList(itemsList, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(COUNT_FIELD.getPreferredName(), count);
            builder.startArray(RESULT_FIELD.getPreferredName());
            {
                for (SynonymSetListItem synonymSetListItem : itemsList) {
                    synonymSetListItem.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymSetList response = (SynonymSetList) o;
        return itemsList.equals(response.itemsList) && count.equals(response.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemsList, count);
    }
}
