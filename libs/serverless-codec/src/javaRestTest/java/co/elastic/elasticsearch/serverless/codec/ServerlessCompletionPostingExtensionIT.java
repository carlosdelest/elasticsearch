/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.serverless.codec;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ServerlessCompletionPostingExtensionIT extends ESRestTestCase {
    private static final String COMPLETION_FST_ON_HEAP_NAME = "serverless.lucene.codec.completion_fst_on_heap";
    private static final MutableSettingsProvider COMPLETION_POSTING_SETTINGS_PROVIDER = new MutableSettingsProvider() {
        {
            put(COMPLETION_FST_ON_HEAP_NAME, "false");
        }
    };

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("logger.co.elastic.elasticsearch.serverless.codec.Elasticsearch900Lucene100CompletionPostingsFormat", "TRACE")
        .systemProperty("serverless.codec.configurable_completions_postings_enabled", "true")
        .settings(COMPLETION_POSTING_SETTINGS_PROVIDER)
        .user("admin-user", "x-pack-test-password")
        .build();

    public void testFstOnHeapIsConfigurable() throws Exception {
        var index = "test_index";
        createCompletionIndex(index);
        ensureGreen(index);
        indexDocument(index);
        refresh(index);

        // The index node should load the segment after the refresh:
        try (InputStream log = cluster.getNodeLog(0, LogType.SERVER)) {
            final List<String> logLines = Streams.readAllLines(log);
            assertTrue(
                "The Index node should load FSTs off heap.",
                logLines.stream().anyMatch(l -> l.contains("[index-") && l.contains("Off Heap fieldsProducer called"))
            );
        }

        searchCompletions(index);

        // The search node should load the segment:
        try (InputStream log = cluster.getNodeLog(1, LogType.SERVER)) {
            final List<String> logLines = Streams.readAllLines(log);
            assertTrue(
                "The Search node should load FSTs off heap as a result of the node config.",
                logLines.stream().anyMatch(l -> l.contains("Off Heap fieldsProducer called"))
            );
        }

        COMPLETION_POSTING_SETTINGS_PROVIDER.put(COMPLETION_FST_ON_HEAP_NAME, "true");
        restartCluster();
        ensureGreen(index);

        // The index node should load the segment:
        try (InputStream log = cluster.getNodeLog(0, LogType.SERVER)) {
            final List<String> logLines = Streams.readAllLines(log);
            assertTrue(
                "The Index node should always load FSTs off heap.",
                logLines.stream().anyMatch(l -> l.contains("Off Heap fieldsProducer called"))
            );
        }

        searchCompletions(index);

        // The search node should load the segment:
        try (InputStream log = cluster.getNodeLog(1, LogType.SERVER)) {
            final List<String> logLines = Streams.readAllLines(log);
            assertTrue(
                "The Search node should load FSTs on heap as a result of the node config.",
                logLines.stream().anyMatch(l -> l.contains("On Heap fieldsProducer called"))
            );
        }
    }

    private void restartCluster() throws Exception {
        cluster.restart(false);
        closeClients();
        initClient();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private void createCompletionIndex(String index) throws IOException {
        final String mappings = """
            "properties": {
               "completion_field": { "type": "completion" }
             }
            """;
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        createIndex(index, settings, mappings);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("completion_field", "test completion field").endObject())
        );
        assertOK(client().performRequest(indexRequest));
    }

    private void searchCompletions(String index) throws IOException {
        Request searchRequest = newXContentRequest(HttpMethod.POST, "/" + index + "/" + "_search", (body, params) -> {
            body.startObject("suggest");
            {
                body.startObject("results");
                {
                    body.field("prefix", "test");
                    body.startObject("completion");
                    body.field("field", "completion_field");
                    body.endObject();
                }
                body.endObject();
            }
            body.endObject();
            return body;
        });
        searchRequest.addParameter("format", "json");
        var response = client().performRequest(searchRequest);
        assertOK(response);
    }
}
