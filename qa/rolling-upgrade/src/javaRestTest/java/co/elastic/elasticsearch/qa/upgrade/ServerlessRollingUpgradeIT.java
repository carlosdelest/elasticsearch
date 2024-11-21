/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.upgrade;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.serverless.ServerlessBwcVersion;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessRollingUpgradeIT extends ESRestTestCase {

    private static final MutableSettingsProvider NEW_CLUSTER_UPLOAD_MAX_COMMITS_SETTINGS_PROVIDER = new MutableSettingsProvider();

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .version(ServerlessBwcVersion.instance())
        // need to set stateless enable explicitly until old version for rolling upgrade has the defaults file
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .settings(NEW_CLUSTER_UPLOAD_MAX_COMMITS_SETTINGS_PROVIDER)
        // TODO remove the settings overrides for indices.memory.interval and indices.disk.interval here and below (ES-9563)
        .setting("indices.memory.interval", "1h")
        .setting("indices.disk.interval", "-1")
        .user("admin-user", "x-pack-test-password")
        .withNode(
            indexNodeSpec -> indexNodeSpec.name("index-node-2")
                .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
                .setting("indices.memory.interval", "1h")
                .setting("indices.disk.interval", "-1")
        )
        .withNode(
            searchNodeSpec -> searchNodeSpec.name("search-node-2")
                .setting("node.roles", "[remote_cluster_client,search]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
                .setting("indices.memory.interval", "1h")
                .setting("indices.disk.interval", "-1")
        )
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testClusterUpgrade() throws Exception {
        // Two indices since we have two indexing nodes
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        int docCount = randomIntBetween(10, 50);

        createIndex(index1, Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0").build());
        createIndex(index2, Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0").build());
        for (int i = 0; i < docCount; i++) {
            indexDocument(index1);
            indexDocument(index2);
        }
        performUpgrade();
        waitForNodes(4);
        ensureGreen(index1);
        ensureGreen(index2);
        assertDocCount(client(), index1, docCount);
        assertDocCount(client(), index2, docCount);
    }

    // TODO remove this test (ES-9563)
    public void testFastRefreshUpgrade() throws Exception {
        record NodeDetails(String name, int indexInCluster, Node clientNode) {}
        ;
        final var indexNodes = new ArrayList<NodeDetails>(2);
        final var searchNodes = new ArrayList<NodeDetails>(2);
        Runnable updateNodes = () -> {
            try {
                closeClients();
                initClient();
                waitForNodes(4);
                indexNodes.clear();
                searchNodes.clear();
                assertThat(cluster.getNumNodes(), equalTo(4));
                for (int i = 0; i < cluster.getNumNodes(); i++) {
                    String nodeName = cluster.getName(i);
                    if (nodeName.contains("search")) {
                        searchNodes.add(new NodeDetails(nodeName, i, client().getNodes().get(i)));
                    } else {
                        indexNodes.add(new NodeDetails(nodeName, i, client().getNodes().get(i)));
                    }
                }
                logger.info("Updated nodes: indexNodes: {}, searchNodes: {}", indexNodes, searchNodes);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
        updateNodes.run();

        final String indexName = "idx-1";
        createIndex(
            indexName,
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1h")
                // exclude the second index and search nodes, so we know the index starts on the first index and search nodes
                .put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodes.get(1).name + "," + searchNodes.get(1).name)
                .build()
        );
        ensureGreen(indexName);

        // Index documents
        logger.info("Indexing two docs");
        client().setNodes(indexNodes.stream().map(n -> n.clientNode).toList());
        createDocument(indexName, "doc1", "f", "v1");
        refresh(indexName);
        createDocument(indexName, "doc2", "f", "v2");

        // Checking the two documents
        logger.info("Checking two docs");
        client().setNodes(searchNodes.stream().map(n -> n.clientNode).toList());
        assertDocCount(client(), indexName, 1);
        assertThat(searchDocuments(indexName), equalTo(1));
        assertThat(getDocuments(indexName, List.of("doc1", "doc2"), false), equalTo(1));
        // The following causes a refresh and sets trackTranslogLocation to true
        assertThat(getDocuments(indexName, List.of("doc1", "doc2"), true), equalTo(2));

        // Index one more doc
        logger.info("Index a third doc");
        client().setNodes(indexNodes.stream().map(n -> n.clientNode).toList());
        createDocument(indexName, "doc3", "f", "v3");

        // From here on, we check the three documents, where doc3 is got by non-real-time mGETs only after the index is refreshed
        Consumer<Boolean> checkDocuments = (doc3NonRealTimeVisible) -> {
            try {
                client().setNodes(searchNodes.stream().map(n -> n.clientNode).toList());
                int expectedNonRealTimeDocs = doc3NonRealTimeVisible ? 3 : 2;
                assertDocCount(client(), indexName, expectedNonRealTimeDocs);
                assertThat(getDocuments(indexName, List.of("doc1", "doc2", "doc3"), false), equalTo(expectedNonRealTimeDocs));
                assertThat(getDocuments(indexName, List.of("doc1", "doc2", "doc3"), true), equalTo(3));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        Consumer<Integer> upgradeNode = (indexInCluster) -> {
            try {
                cluster.upgradeNodeToVersion(indexInCluster, Version.CURRENT);
                updateNodes.run();
                ensureGreen(indexName);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        logger.info("Checking docs");
        checkDocuments.accept(false);

        // Upgrade first search node (that has the search shard)
        upgradeNode.accept(searchNodes.get(0).indexInCluster);

        logger.info("Checking docs after upgrading first search node");
        checkDocuments.accept(false);

        // Upgrade second search node (that does not have the search shard)
        upgradeNode.accept(searchNodes.get(1).indexInCluster);

        logger.info("Checking docs after upgrading second search node");
        checkDocuments.accept(false);

        // Upgrade first index node (that has the index shard and will flush)
        upgradeNode.accept(indexNodes.get(0).indexInCluster);

        logger.info("Checking docs after upgrading first index node");
        checkDocuments.accept(true);

        // Upgrade second index node, completing the cluster upgrade and thus searches happen on the search nodes
        upgradeNode.accept(indexNodes.get(1).indexInCluster);

        logger.info("Checking docs after upgrading second index node");
        checkDocuments.accept(true);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        assertOK(client().performRequest(indexRequest));
    }

    private void createDocument(String index, String id, String fieldName, String fieldValue) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_create/" + id);
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field(fieldName, fieldValue).endObject()));
        assertOK(client().performRequest(indexRequest));
    }

    private int getDocuments(String index, List<String> ids, boolean realtime) throws IOException {
        Request request = new Request("GET", "/" + index + "/" + "_mget?realtime=" + realtime);
        var json = JsonXContent.contentBuilder().startObject().startArray("docs");
        for (String id : ids) {
            json.startObject().field("_id", id).endObject();
        }
        json.endArray().endObject();
        String jsonString = Strings.toString(json);
        request.setJsonEntity(jsonString);
        Response response = client().performRequest(request);
        assertOK(response);
        String responseString = EntityUtils.toString(response.getEntity());
        var jsonResponse = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseString, false);
        var docs = (List) jsonResponse.get("docs");
        var foundDocs = 0;
        for (Object doc : docs) {
            var docMap = (Map) doc;
            if ((Boolean) docMap.get("found")) {
                foundDocs++;
            }
        }
        return foundDocs;
    }

    private Integer searchDocuments(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/" + "_search");
        Response response = client().performRequest(request);
        assertOK(response);
        var jsonResponse = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        var hits = (Map) jsonResponse.get("hits");
        var hitsTotal = (Map) hits.get("total");
        return Integer.parseInt(hitsTotal.get("value").toString());
    }

    private void performUpgrade() throws IOException {
        randomUploadMaxCommitsSettingsForNewCluster();
        cluster.upgradeToVersion(Version.CURRENT);
        closeClients();
        initClient();
    }

    private static void randomUploadMaxCommitsSettingsForNewCluster() {
        NEW_CLUSTER_UPLOAD_MAX_COMMITS_SETTINGS_PROVIDER.put(
            "stateless.upload.max_commits",
            randomBoolean() ? String.valueOf(between(1, 10)) : "100"
        );
    }

    private static void waitForNodes(int numberOfNodes) throws Exception {
        assertBusy(() -> {
            Request nodesRequest = new Request("GET", "/_nodes");
            ObjectPath nodesPath = assertOKAndCreateObjectPath(client().performRequest(nodesRequest));
            assertThat(nodesPath.evaluate("_nodes.total"), equalTo(numberOfNodes));
        });
    }
}
