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

package co.elastic.elasticsearch.lifecycle.serverless;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;

public class ServerlessDataStreamLifecycleIT extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testILMPolicesAreNotInstalled() throws Exception {
        try (var client = client()) {
            ensureGreen("*");

            // let's wait for a few component templates to be installed
            assertBusy(() -> {
                Response templatesResponse = client.performRequest(new Request("GET", "_component_template"));
                String componentTemplatesResponse = EntityUtils.toString(templatesResponse.getEntity());
                assertThat(componentTemplatesResponse, containsString("metrics-settings"));
                assertThat(componentTemplatesResponse, containsString("logs-settings"));
            }, 30, TimeUnit.SECONDS);

            // let's test no component template configures index.lifecycle.name
            assertBusy(() -> {
                Response templatesResponse = client.performRequest(new Request("GET", "_component_template"));
                assertThat(EntityUtils.toString(templatesResponse.getEntity()), not(containsString("index.lifecycle.name")));
            }, 30, TimeUnit.SECONDS);

            // let's test no ILM policy exists in the cluster state
            Response clusterStateResponse = client.performRequest(new Request("GET", "_cluster/state"));
            assertThat(
                "in serverless we run in data stream lifecycle only mode so no ILM policies should be installed",
                EntityUtils.toString(clusterStateResponse.getEntity()),
                not(containsString("index_lifecycle"))
            );
        }
    }
}
