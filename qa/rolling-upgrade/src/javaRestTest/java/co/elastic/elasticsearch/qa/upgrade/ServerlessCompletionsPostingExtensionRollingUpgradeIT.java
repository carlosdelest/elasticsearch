/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.upgrade;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessBwcVersion;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ServerlessCompletionsPostingExtensionRollingUpgradeIT extends ServerlessParameterizedRollingUpgradeTestCase {
    private static String newCodecEnabled = "false";

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .version(ServerlessBwcVersion.instance())
        // We need to set stateless.enabled explicitly here until old version for rolling upgrade has the defaults file.
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .systemProperty("serverless.codec.configurable_completions_postings_enabled", newCodecEnabled)
        .user("admin-user", "x-pack-test-password")
        .withNode(
            indexNodeSpec -> indexNodeSpec.name("index-node-2") // The first index node is created by default.
                .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        )
        .withNode(
            searchNodeSpec -> searchNodeSpec.name("search-node-2") // The first search node is created by default.
                .setting("node.roles", "[remote_cluster_client,search]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        )
        .build();

    public ServerlessCompletionsPostingExtensionRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * This test verifies that the completions codec and surrounding infrastructure is safe to use during a rolling upgrade. Each iteration
     * of the test creates a new index, and then indexes a few docs into it, as well as into the previously created indices. Once the
     * cluster is fully upgraded, the test enables the new codec, creates a new index, then checks that all completions are still searchable
     * using the current (new codec) and previously created (old codec) indices.
     *
     * @throws Exception
     */
    public void testClusterUpgrade() throws Exception {
        if (isUpgradedCluster()) {
            // Once the cluster is upgraded, apply the new codec.
            newCodecEnabled = "true";
            cluster.restart(false);
            closeClients();
            initClient();
        }

        int testIteration = getNumberOfUpgradedNodes();
        var testRuns = testIteration + 1;

        String indexBaseName = "test-idx-completions";

        String newIndex = indexBaseName + "-" + testIteration;
        createCompletionIndex(newIndex);
        ensureGreen(newIndex);

        // Index a few suggestions per index at each step of the rolling upgrade process.
        int docsIndexedPerIteration = 5;
        for (int indexNum = 0; indexNum < testRuns; indexNum++) {

            String currentIndex = indexBaseName + "-" + indexNum;
            for (int docNum = 0; docNum < docsIndexedPerIteration; docNum++) {
                indexCompletionText(currentIndex, "testtext_" + testIteration + "_" + docNum);
            }
            refresh(currentIndex);

            // Test that all current and previously indexed suggestions are searchable.
            Response response = searchCompletions(currentIndex);
            Map<String, Object> respMap = responseAsMap(response);
            var suggestions = extractSuggestions(respMap);

            var expectedSuggestions = docsIndexedPerIteration * (testRuns - indexNum);
            var actualSuggestions = suggestions.size();
            assertEquals(
                "The number of suggestions returned should match the number that were indexed",
                expectedSuggestions,
                actualSuggestions
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractSuggestions(Map<String, Object> respMap) {
        var suggestResults = (List<Map<String, ?>>) XContentMapValues.extractValue("suggest.results", respMap);
        return suggestResults.stream()
            .flatMap(
                result -> ((List<Map<String, ?>>) XContentMapValues.extractValue("options", result)).stream()
                    .map(suggestion -> (String) suggestion.get("text"))

            )
            .toList();
    }

    private void createCompletionIndex(String index) throws IOException {
        final String mappings = """
            "properties": {
               "completion_field": { "type": "completion" }
             }
            """;
        Settings settings = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 2))
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 2))
            .build();
        createIndex(index, settings, mappings);
    }

    private void indexCompletionText(String index, String completionText) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.format("""
            {
              "completion_field" : {
                "input": "%s"
              }
            }
            """, completionText));
        assertOK(client().performRequest(indexRequest));
    }

    private Response searchCompletions(String index) throws IOException {
        Request searchRequest = new Request("POST", "/" + index + "/" + "_search");
        searchRequest.setJsonEntity("""
            {
              "suggest": {
                "results": {
                  "prefix": "testt",
                  "completion": {
                    "field": "completion_field",
                    "size": 100
                  }
                }
              }
            }
            """);
        searchRequest.addParameter("format", "json");
        var response = client().performRequest(searchRequest);
        assertOK(response);
        return response;
    }
}
