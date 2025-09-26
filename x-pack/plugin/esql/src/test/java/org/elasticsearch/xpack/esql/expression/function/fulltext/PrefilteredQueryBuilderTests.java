/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.ESKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class PrefilteredQueryBuilderTests extends AbstractQueryTestCase<PrefilteredQueryBuilder> {

    private static final String VECTOR_FIELD = "vector";
    public static final int VECTOR_DIMENSIONS = 64;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(EsqlPlugin.class);
    }

    @Override
    protected PrefilteredQueryBuilder doCreateTestQueryBuilder() {
        float[] vector = randomVector();
        QueryBuilder innerQuery = new SemanticQueryBuilder(VECTOR_FIELD, vector, 10, null, null, null, null);
//        QueryBuilder innerQuery = new KnnVectorQueryBuilder(VECTOR_FIELD, vector, 10, null, null, null, null);
//        QueryBuilder innerQuery = randomBoolean() ?
//            new KnnVectorQueryBuilder(VECTOR_FIELD, vector, 10, null, null, null, null) :
//            RandomQueryBuilder.createQuery(random());

        List<QueryBuilder> filters = randomList(1, 3, () -> RandomQueryBuilder.createQuery(random()));
        return new PrefilteredQueryBuilder(innerQuery, filters);
    }

    private static float[] randomVector() {
        float[] vector = new float[VECTOR_DIMENSIONS];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    @Override
    protected void doAssertLuceneQuery(PrefilteredQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {

        if (queryBuilder.getInnerQuery() instanceof KnnVectorQueryBuilder knnVectorQueryBuilder) {
            assertTrue(query instanceof ESKnnFloatVectorQuery);
            ESKnnFloatVectorQuery knnQuery = (ESKnnFloatVectorQuery) query;
            assertNotNull(knnQuery.getFilter());

        } else {
            assertFalse(query instanceof ESKnnFloatVectorQuery);
        }
    }

    protected PrefilteredQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        List<QueryBuilder> filters = randomList(1, 3, () -> RandomQueryBuilder.createQuery(random()));
        return new PrefilteredQueryBuilder(queryBuilder, filters);
    }

    @Override
    public void testMustRewrite() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, super::testMustRewrite);
        assertThat(e.getMessage(), containsString("should have been rewritten to another query type"));
    }

    @Override
    public void testUnknownField() {
        assumeTrue("test doesn't apply for prefilter queries", false);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSIONS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(Strings.toString(builder)),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }
}
