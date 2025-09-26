/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.FilteredQueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.PrefilteredQuery;

import java.util.List;
import java.util.stream.Collectors;

public abstract class PrefilteredFullTextFunction extends FullTextFunction {

    // Expressions to be used as prefilters in knn query
    private final List<Expression> filterExpressions;

    protected PrefilteredFullTextFunction(
        Source source,
        Expression query,
        List<Expression> children,
        QueryBuilder queryBuilder,
        List<Expression> filterExpressions
    ) {
        super(source, query, children, queryBuilder);
        this.filterExpressions = filterExpressions;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        Translatable translatable = super.translatable(pushdownPredicates);
        if (queryBuilder() instanceof FilteredQueryBuilder<?> == false) {
            // We don't have prefilters in the query builder, so no need to check further
            return translatable;
        }
        // We need to check whether filter expressions are translatable as well
        for (Expression filterExpression : prefilterExpressions()) {
            translatable = translatable.merge(TranslationAware.translatable(filterExpression, pushdownPredicates));
        }

        return translatable;
    }

    public abstract Expression withPrefilters(List<Expression> filterExpressions);

    public List<Expression> prefilterExpressions() {
        return filterExpressions;
    }

    @Override
    public final Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        PrefilteredQuery prefilteredQuery = new PrefilteredQuery(
            source(),
            translateInnerQuery(pushdownPredicates, handler),
            prefiltersAsQueries(pushdownPredicates, handler)
        );

        return prefilteredQuery;
    }

    protected abstract Query translateInnerQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler);

    private List<Query> prefiltersAsQueries(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return prefilterExpressions()
            .stream()
            // We can only translate filter expressions that are translatable. In case any is not translatable,
            // Knn won't be pushed down so it's safe not to translate all filters and check them when creating an evaluator
            // for the non-pushed down query
            .filter(f -> f instanceof TranslationAware)
            .map(TranslationAware.class::cast)
            .map(ta -> ta.asQuery(pushdownPredicates, handler))
            .collect(Collectors.toList());
    }
}
