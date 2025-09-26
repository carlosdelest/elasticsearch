/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.PrefilteredQueryBuilder;

import java.util.List;
import java.util.Objects;

public class PrefilteredQuery extends Query {

    private final Query innerQuery;
    private final List<Query>  prefilters;

    public PrefilteredQuery(Source source, Query innerQuery, List<Query> prefilters) {
        super(source);
        this.innerQuery = innerQuery;
        this.prefilters = prefilters;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new PrefilteredQueryBuilder(innerQuery.toQueryBuilder(),
            prefilters.stream().map(Query::toQueryBuilder).toList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerQuery, prefilters);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        PrefilteredQuery other = (PrefilteredQuery) obj;
        return Objects.equals(innerQuery, other.innerQuery)
            && Objects.equals(prefilters, other.prefilters);
    }

    @Override
    protected String innerToString() {
        return innerQuery + " [" + prefilters + "]";
    }

    @Override
    public boolean scorable() {
        return innerQuery.scorable();
    }

    @Override
    public boolean containsPlan() {
        return false;
    }
}
