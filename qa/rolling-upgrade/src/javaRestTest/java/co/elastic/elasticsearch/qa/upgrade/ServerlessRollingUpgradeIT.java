/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.upgrade;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.serverless.ServerlessBwcVersion;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

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
        .user("admin-user", "x-pack-test-password")
        .withNode(
            indexNodeSpec -> indexNodeSpec.name("index-node-2")
                .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        )
        .withNode(
            searchNodeSpec -> searchNodeSpec.name("search-node-2")
                .setting("node.roles", "[remote_cluster_client,search]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
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

        // Repository and snapshot remain valid after upgrade
        String repoName = "test-repo";
        String snapshotName = "test-snapshot";
        registerRepository(repoName, FsRepository.TYPE, randomBoolean(), Settings.builder().put("location", "backup").build());
        createSnapshot(repoName, snapshotName, true);
        var repoAndSnapshotsMap = getRepoAndSnapshotsMap(repoName);

        performUpgrade();
        waitForNodes(4);
        ensureGreen(index1);
        ensureGreen(index2);
        assertDocCount(client(), index1, docCount);
        assertDocCount(client(), index2, docCount);
        assertThat(getRepoAndSnapshotsMap(repoName), equalTo(repoAndSnapshotsMap));
        deleteIndex(index1);
        restoreSnapshotWithGlobalState(repoName, snapshotName, index1);
        ensureGreen(index1);
        assertDocCount(client(), index1, docCount);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        assertOK(client().performRequest(indexRequest));
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

    private Tuple<Map<String, Object>, Map<String, Object>> getRepoAndSnapshotsMap(String repoName) throws Exception {
        Response getRepoResponse = client().performRequest(new Request("GET", "/_snapshot/" + repoName));
        assertOK(getRepoResponse);
        Response getSnapshotsResponse = client().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all"));
        assertOK(getSnapshotsResponse);
        return new Tuple<>(responseAsMap(getRepoResponse), responseAsMap(getSnapshotsResponse));
    }

    private void restoreSnapshotWithGlobalState(String repoName, String snapshotName, String indexName) throws Exception {
        final Request request = new Request("POST", "/_snapshot/" + repoName + '/' + snapshotName + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": true
            }""", indexName));
        final Response response = client().performRequest(request);
        assertOK(response);
    }
}
