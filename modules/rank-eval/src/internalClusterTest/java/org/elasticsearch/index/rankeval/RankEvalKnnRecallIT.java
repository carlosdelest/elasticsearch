/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class RankEvalKnnRecallIT extends ESIntegTestCase {

    private static final String INDEX = "knn_test";
    private static final String VECTOR_FIELD = "vector";
    private static final int DIM = 4;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RankEvalPlugin.class, PainlessPlugin.class, MustachePlugin.class);
    }

    public void testRecallAt10WithRatingsProvider() throws Exception {
        // Create index with dense_vector field
        XContentBuilder mapping = org.elasticsearch.xcontent.XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject(VECTOR_FIELD)
                        .field("type", "dense_vector")
                        .field("dims", DIM)
                        .field("index", true)
                        .field("similarity", "cosine")
                    .endObject()
                    .startObject("category")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();

        assertAcked(
            client().admin().indices().create(new CreateIndexRequest(INDEX).mapping(mapping)).actionGet()
        );

        // Index some documents with vectors
        float[][] vectors = {
            {1, 0, 0, 0},
            {0, 1, 0, 0},
            {0, 0, 1, 0},
            {0, 0, 0, 1},
            {1, 1, 0, 0},
            {1, 0, 1, 0},
            {1, 0, 0, 1},
            {0, 1, 1, 0},
            {0, 1, 0, 1},
            {0, 1, 1, 1}
        };
        for (int i = 0; i < vectors.length; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put(VECTOR_FIELD, vectors[i]);
            client().index(new IndexRequest(INDEX).id(String.valueOf(i + 1)).source(doc)).actionGet();
        }
        refresh(INDEX);

        // Query vector: [1, 1, 0, 0] (should match docs 1,2,5 most closely)
        float[] queryVector = new float[] {1, 1, 0, 0};

        // KNN template
        String knnTemplate = """
          {
              "query": {
                "knn": {
                  "field": "vector",
                  "query_vector": {{query_vector}},
                  "k": 10,
                  "num_candidates": 10
                }
              }
          }
        """;

        // Exact NN template (script_score)
        String exactTemplate = """
          {
              "query": {
                "script_score": {
                  "query": {
                    "match_all": {}
                  },
                  "script": {
                    "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                    "params": {
                      "query_vector": {{query_vector}}
                    }
                  }
                }
              }
          }
        """;

        // Prepare templates
        List<RankEvalSpec.ScriptWithId> templates = List.of(
            new RankEvalSpec.ScriptWithId("knn_query", new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, knnTemplate, Map.of())),
            new RankEvalSpec.ScriptWithId(
                "exact_nn_query",
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, exactTemplate, Map.of())
            )
        );

        // Prepare RatedRequest with RatingsProvider
        Map<String, Object> params = Map.of("query_vector", Arrays.toString(queryVector));
        RatedRequest.RatingsProvider ratingsProvider = new RatedRequest.RatingsProvider("exact_nn_query", params);

        RatedRequest knnRequest = new RatedRequest(
            "recall_query",
            List.of(), // ratedDocs will be filled by RatingsProvider
            null,
            Map.of("query_vector", Arrays.toString(queryVector)),
            "knn_query",
            ratingsProvider
        );

        RecallAtK metric = new RecallAtK(1, 10);

        RankEvalSpec spec = new RankEvalSpec(List.of(knnRequest), metric, templates);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), new RankEvalRequest());
        builder.setRankEvalSpec(spec);
        builder.request().indices(INDEX);

        RankEvalResponse response = client().execute(RankEvalPlugin.ACTION, builder.request()).actionGet();

        // Recall@10 should be 1.0 since both queries return the same docs
        assertThat(response.getFailures().size(), equalTo(0));
        assertEquals(1.0, response.getMetricScore(), 0.0001);
        EvalQueryQuality recallQuery = response.getPartialResults().get("recall_query");
        assertNotNull(recallQuery);
        List<RatedSearchHit> hitsAndRatings = recallQuery.getHitsAndRatings();
        assertThat(hitsAndRatings.size(), equalTo(10));
        assertTrue(hitsAndRatings.stream().allMatch(h -> h.getRating().getAsInt() == 1));
    }
}

