/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.inference.InferenceAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.SemanticTextFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.SEMANTIC_TEXT_FIELD_ADDED;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {

    public static final String NAME = "semantic_query";

    private final String fieldName;
    private final String query;

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private SetOnce<InferenceResults> inferenceResultsSupplier;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a query");
        }
        this.fieldName = fieldName;
        this.query = query;
    }

    public SemanticQueryBuilder(SemanticQueryBuilder other, SetOnce<InferenceResults> inferenceResultsSupplier) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (inferenceResultsSupplier != null) {
            if (inferenceResultsSupplier.get() == null) {
                // Inference still not returned
                return this;
            }
            return inferenceResultsToQuery(fieldName, inferenceResultsSupplier.get());
        }

        String modelName = queryRewriteContext.modelNameForField(fieldName);
        if (modelName == null) {
            throw new IllegalArgumentException(
                "field [" + fieldName + "] is not a " + SemanticTextFieldMapper.CONTENT_TYPE + " field type"
            );

        }
        // TODO Hardcoding task type
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(TaskType.SPARSE_EMBEDDING, modelName, query, Map.of());

        SetOnce<InferenceResults> inferenceResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            executeAsyncWithOrigin(client, ML_ORIGIN, InferenceAction.INSTANCE, inferenceRequest, ActionListener.wrap(inferenceResponse -> {
                inferenceResultsSupplier.set(inferenceResponse.getResult());
            }, listener::onFailure));
        });

        return new SemanticQueryBuilder(this, inferenceResultsSupplier);
    }

    private static QueryBuilder inferenceResultsToQuery(String fieldName, InferenceResults inferenceResults) {
        if (inferenceResults instanceof TextExpansionResults expansionResults) {
            var boolQuery = QueryBuilders.boolQuery();
            for (var weightedToken : expansionResults.getWeightedTokens()) {
                boolQuery.should(QueryBuilders.termQuery(fieldName, weightedToken.token()).boost(weightedToken.weight()));
            }
            boolQuery.minimumShouldMatch(1);
            return boolQuery;
        } else {
            throw new IllegalArgumentException(
                "field [" + fieldName + "] does not use a model that outputs sparse vector inference results"
            );
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SEMANTIC_TEXT_FIELD_ADDED;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceResultsSupplier != null) {
            throw new IllegalStateException("inference supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException("semantic_query should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(inferenceResultsSupplier, other.inferenceResultsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResultsSupplier);
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            query = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No field name specified for semantic query");
        }

        if (query == null) {
            throw new ParsingException(parser.getTokenLocation(), "No query specified for semantic query");
        }

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(fieldName, query);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
