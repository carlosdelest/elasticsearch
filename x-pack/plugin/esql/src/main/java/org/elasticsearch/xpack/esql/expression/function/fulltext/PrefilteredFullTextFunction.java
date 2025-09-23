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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates.DEFAULT;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

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

    public final Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof FilteredQueryBuilder<?> filteredQueryBuilder) {
            queryBuilder = filteredQueryBuilder.addFilterQueries(prefilterQueryBuilders());
        }

        return replaceFilteredQueryBuilder(queryBuilder);
    }

    private List<QueryBuilder> prefilterQueryBuilders() {
        List<QueryBuilder> filterQueries = new ArrayList<>();
        for (Expression filterExpression : prefilterExpressions()) {
            if (filterExpression instanceof TranslationAware translationAware) {
                // We can only translate filter expressions that are translatable. In case any is not translatable,
                // Knn won't be pushed down so it's safe not to translate all filters and check them when creating an evaluator
                // for the non-pushed down query
                if (translationAware.translatable(DEFAULT) == Translatable.YES) {
                    filterQueries.add(TRANSLATOR_HANDLER.asQuery(DEFAULT, filterExpression).toQueryBuilder());
                }
            }
        }

        return filterQueries;
    }

    protected abstract Expression replaceFilteredQueryBuilder(QueryBuilder queryBuilder);
}
