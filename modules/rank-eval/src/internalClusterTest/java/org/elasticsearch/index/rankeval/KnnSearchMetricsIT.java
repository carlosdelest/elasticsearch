/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.rankeval.SearchRankEvalActionFilter.RECALL_METRIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class KnnSearchMetricsIT extends ESSingleNodeTestCase {

    private static final String INDEX_NAME = "test_knn_index";
    private static final String VECTOR_FIELD = "vector";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, RankEvalPlugin.class);
    }

    private XContentBuilder createKnnMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", 2)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        getTestTelemetryPlugin().resetMeter();
    }

    public void testKnnSectionSearch() throws Exception {
        final int numShards = randomIntBetween(1, 3);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards))
            .setMapping(createKnnMapping())
            .get();

        final int count = 100;
        for (int i = 0; i < count; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, new float[] { i * 0.1f, i * 0.1f })
                .field("category", i >= 90 ? "last_ten" : null)
                .endObject();
            client.prepareIndex(INDEX_NAME).setSource(source).get();
        }

        indicesAdmin().prepareRefresh(INDEX_NAME)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .get();

        final int k = randomIntBetween(11, 15);
        // test top level knn search
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null)));

            SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client()).setIndices(INDEX_NAME).setSource(sourceBuilder);
            assertNoFailures(requestBuilder);

            assertBusy(() -> {
                List<Measurement> measurements = getTestTelemetryPlugin().getDoubleHistogramMeasurement(RECALL_METRIC);
                assertFalse("No recall measurements found", measurements.isEmpty());
            });
        }

        // test knn query
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, null, null, null));

            SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client()).setIndices(INDEX_NAME).setSource(sourceBuilder);
            assertNoFailures(requestBuilder);

            assertBusy(() -> {
                List<Measurement> measurements = getTestTelemetryPlugin().getDoubleHistogramMeasurement(RECALL_METRIC);
                assertFalse("No recall measurements found for knn query", measurements.isEmpty());
            });
        }
    }

    public void testKnnQuerySearch() throws Exception {
        final int numShards = randomIntBetween(1, 3);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards))
            .setMapping(createKnnMapping())
            .get();

        final int count = 100;
        for (int i = 0; i < count; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, new float[] { i * 0.1f, i * 0.1f })
                .field("category", i >= 90 ? "last_ten" : null)
                .endObject();
            client.prepareIndex(INDEX_NAME).setSource(source).get();
        }

        indicesAdmin().prepareRefresh(INDEX_NAME)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .get();

        final int k = randomIntBetween(11, 15);
        // test top level knn search
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null)));

            SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client()).setIndices(INDEX_NAME).setSource(sourceBuilder);
            assertNoFailures(requestBuilder);

            assertBusy(() -> {
                List<Measurement> measurements = getTestTelemetryPlugin().getDoubleHistogramMeasurement(RECALL_METRIC);
                assertFalse("No recall measurements found", measurements.isEmpty());
            });
        }
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }
}
