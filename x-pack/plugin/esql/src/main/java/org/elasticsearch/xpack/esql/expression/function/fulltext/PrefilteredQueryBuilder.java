/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class PrefilteredQueryBuilder extends AbstractQueryBuilder<PrefilteredQueryBuilder> {

    private static final TransportVersion PREFILTERED_QUERY_BUILDER = TransportVersion.fromName("prefiltered_query_builder");

    public static final String NAME = "prefilter";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FILTER_FIELD = new ParseField("filter");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PrefilteredQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> new PrefilteredQueryBuilder((QueryBuilder) args[0], (List<QueryBuilder>) args[1])
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), QUERY_FIELD);
        PARSER.declareFieldArray(
            constructorArg(),
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        declareStandardFields(PARSER);
    }

    private final QueryBuilder innerQuery;
    private final List<QueryBuilder> filters;

    public PrefilteredQueryBuilder(QueryBuilder innerQuery, List<QueryBuilder> filters) {
        assert innerQuery != null : "inner query cannot be null";
        assert filters != null && filters.isEmpty() == false : "filters cannot be null and can't be empty";

        this.innerQuery = innerQuery;
        this.filters = filters;
    }

    public PrefilteredQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.innerQuery = in.readNamedWriteable(QueryBuilder.class);
        this.filters = in.readNamedWriteableCollectionAsList(QueryBuilder.class);
    }

    QueryBuilder getInnerQuery() {
        return innerQuery;
    }

    List<QueryBuilder> getFilters() {
        return filters;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(innerQuery);
        out.writeNamedWriteableCollection(filters);
    }

    public static PrefilteredQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), innerQuery);
        builder.startArray(FILTER_FIELD.getPreferredName());
        for (QueryBuilder filterQuery : filters) {
            filterQuery.toXContent(builder, params);
        }
        builder.endArray();
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenInnerQuery = innerQuery.rewrite(queryRewriteContext);
        if (rewrittenInnerQuery != innerQuery) {
            if (rewrittenInnerQuery instanceof NestedQueryBuilder nestedQueryBuilder) {
                QueryBuilder nestedInnerQuery = nestedQueryBuilder.query();
                if (nestedInnerQuery instanceof KnnVectorQueryBuilder knnVectorQueryBuilder) {
                    knnVectorQueryBuilder.addFilterQueries(filters);
                }
            }
            return new PrefilteredQueryBuilder(rewrittenInnerQuery, filters);
        }

        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return innerQuery.toQuery(context);
//        throw new IllegalStateException(NAME + " should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(PrefilteredQueryBuilder other) {
        return Objects.equals(innerQuery, other.innerQuery)
            && Objects.equals(filters, other.filters);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(innerQuery, filters);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PREFILTERED_QUERY_BUILDER;
    }
}
