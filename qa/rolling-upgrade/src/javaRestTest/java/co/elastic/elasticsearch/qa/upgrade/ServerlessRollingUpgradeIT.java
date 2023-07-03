/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.upgrade;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster.SERVERLESS_BWC_VERSION;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessRollingUpgradeIT extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .version(SERVERLESS_BWC_VERSION)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testClusterUpgrade() throws Exception {
        String index = "test";
        int docCount = randomIntBetween(10, 50);

        createIndex(index, Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0").build());
        for (int i = 0; i < docCount; i++) {
            indexDocument(index);
        }
        performUpgrade();
        waitForNodes(2);
        ensureGreen(index);
        assertDocCount(client(), index, docCount);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        assertOK(client().performRequest(indexRequest));
    }

    private void performUpgrade() throws IOException {
        cluster.upgradeToVersion(Version.CURRENT);
        closeClients();
        initClient();
    }

    private static void waitForNodes(int numberOfNodes) throws Exception {
        assertBusy(() -> {
            Request nodesRequest = new Request("GET", "/_nodes");
            ObjectPath nodesPath = assertOKAndCreateObjectPath(client().performRequest(nodesRequest));
            assertThat(nodesPath.evaluate("_nodes.total"), equalTo(numberOfNodes));
        });
    }
}
