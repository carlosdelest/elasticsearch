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

package co.elastic.elasticsearch.serverless.datastream;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ServerlessDataStreamLifecycleIT extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testILMPolicesAreNotInstalled() throws Exception {
        ensureGreen("*");

        // let's wait for a few component templates to be installed
        assertBusy(() -> {
            Response templatesResponse = client().performRequest(new Request("GET", "_component_template"));
            String componentTemplatesResponse = EntityUtils.toString(templatesResponse.getEntity());
            assertThat(componentTemplatesResponse, containsString("metrics-settings"));
            assertThat(componentTemplatesResponse, containsString("logs-settings"));
        }, 30, TimeUnit.SECONDS);

        // let's test no component template configures index.lifecycle.name
        assertBusy(() -> {
            Response templatesResponse = client().performRequest(new Request("GET", "_component_template"));
            assertThat(EntityUtils.toString(templatesResponse.getEntity()), not(containsString("index.lifecycle.name")));
        }, 30, TimeUnit.SECONDS);

        // let's test no ILM policy exists in the cluster state
        Response clusterStateResponse = client().performRequest(new Request("GET", "_cluster/state"));
        assertThat(
            "in serverless we run in data stream lifecycle only mode so no ILM policies should be installed",
            EntityUtils.toString(clusterStateResponse.getEntity()),
            not(containsString("index_lifecycle"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testDefaultLifecycleIsConfigured() throws Exception {
        {
            Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/template_without_lifecycle");
            putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"nolifecycle-*\"], \"data_stream\": {}}");
            assertOK(client().performRequest(putComposableIndexTemplateRequest));

            Request createDocRequest = new Request("POST", "/nolifecycle-myapp/_doc?refresh=true");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
            assertOK(client().performRequest(createDocRequest));
        }

        {
            Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/template_with_lifecycle");
            putComposableIndexTemplateRequest.setJsonEntity(
                "{\"index_patterns\": [\"withlifecycle-*\"], "
                    + "\"data_stream\": {}, \"template\": { \"lifecycle\": { \"data_retention\": \"7d\" } } }"
            );
            assertOK(client().performRequest(putComposableIndexTemplateRequest));

            Request createDocRequest = new Request("POST", "/withlifecycle-myapp/_doc?refresh=true");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
            assertOK(client().performRequest(createDocRequest));
        }

        Request getDataStreamsRequest = new Request("GET", "/_data_stream");
        List<Map<String, Object>> getDataStreamsResponse = (List<Map<String, Object>>) entityAsMap(
            client().performRequest(getDataStreamsRequest)
        ).get("data_streams");
        for (Map<String, Object> response : getDataStreamsResponse) {
            if (response.get("name").equals("nolifecycle-myapp")) {
                // we expect the default lifecycle to be configured for this data streams
                assertThat(((Map<String, Object>) response.get("lifecycle")).get("enabled"), is(true));
            } else if (response.get("name").equals("withlifecycle-myapp")) {
                // the explicitly configured (7d retention) lifecycles must remain configured (i.e. the default lifecycle is only
                // configured for data streams without an explicit lifecycle configuration)
                assertThat(((Map<String, Object>) response.get("lifecycle")).get("data_retention"), is("7d"));
                assertThat(((Map<String, Object>) response.get("lifecycle")).get("enabled"), is(true));
            }
        }
    }
}
