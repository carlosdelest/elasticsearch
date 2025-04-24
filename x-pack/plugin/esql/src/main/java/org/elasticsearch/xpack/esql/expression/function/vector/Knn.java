/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KnnQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.K_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.NUM_CANDS_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;
import static org.elasticsearch.search.vectors.RescoreVectorBuilder.OVERSAMPLE_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction.populateOptionsMap;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.Match.getNameFromFieldAttribute;

public class Knn extends Function implements TranslationAware, OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", Knn::readFrom);

    private final Expression field;
    private final Expression query;
    // TODO Options could be serialized via QueryBuilder in case we want to rewrite it in the coordinator node (for query text inference)
    private final Expression options;

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(K_FIELD.getPreferredName(), INTEGER),
        entry(NUM_CANDS_FIELD.getPreferredName(), INTEGER),
        entry(VECTOR_SIMILARITY_FIELD.getPreferredName(), FLOAT),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(OVERSAMPLE_FIELD.getPreferredName(), FLOAT)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Finds the k nearest vectors to a query vector, as measured by a similarity metric.
            knn function finds nearest vectors through approximate search on indexed dense_vectors
            """,
        appliesTo = {
            @FunctionAppliesTo(
                lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT
            ) }
    )
    public Knn(Source source, Expression field, Expression query, Expression options) {
        super(source, options == null ? List.of(field, query) : List.of(field, query, options));
        this.field = field;
        this.query = query;
        this.options = options;
    }

    public Expression field() {
        return field;
    }

    public Expression query() {
        return query;
    }

    public Expression options() {
        return options;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNotNull(field(), sourceText(), FIRST).and(isType(field(), dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"))
            .and(TypeResolutions.isNumeric(query(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
    }

    @Override
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        return true;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = Match.fieldAsFieldAttribute(field());

        Check.notNull(fieldAttribute, "Match must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        @SuppressWarnings("unchecked")
        List<Double> queryFolded = (List<Double>) query().fold(FoldContext.small() /* TODO remove me */);
        float[] queryAsFloats = new float[queryFolded.size()];
        for (int i = 0; i < queryFolded.size(); i++) {
            queryAsFloats[i] = queryFolded.get(i).floatValue();
        }

        return new KnnQuery(source(), fieldName, queryAsFloats, queryOptions());
    }

    private Map<String, Object> queryOptions() throws InvalidArgumentException {
        if (options() == null) {
            return Map.of();
        }

        Map<String, Object> options = new HashMap<>();
        populateOptionsMap((MapExpression) options(), options, THIRD, sourceText(), ALLOWED_OPTIONS);
        return options;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Knn(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Knn::new, field(), query(), options());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Knn readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        Expression options = in.readOptionalNamedWriteable(Expression.class);

        return new Knn(source, field, query, options);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(options());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Knn knn = (Knn) o;
        return Objects.equals(field, knn.field) && Objects.equals(query, knn.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, query);
    }

}
