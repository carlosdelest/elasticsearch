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

package co.elastic.elasticsearch.serverless.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Rule;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RestoreProjectRestTestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(RestoreProjectRestTestIT.class);

    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
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

    public void testSystemIndicesCanBeRestored() throws Exception {

        // create an ordinary index called test_index_1 and put some documents into it
        String testIndex1 = "test_index_1";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        createIndex(testIndex1, settings);

        // and ingest some docs into that index
        int numDocs = 99;
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + testIndex1 + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        ensureGreen(testIndex1);

        client().performRequest(new Request("POST", "/" + testIndex1 + "/_flush"));

        // execute a reindex with wait_for_completion=false to provoke a .tasks index
        String testIndex2 = "test_index_2";
        createIndex(testIndex2, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        Request reindexRequest = new Request("POST", "/_reindex");
        reindexRequest.addParameter("wait_for_completion", "false");
        reindexRequest.setJsonEntity("""
            {
              "source": {
                "index": "test_index_1"
              },
              "dest": {
                "index": "test_index_2"
              }
            }""");
        client().performRequest(reindexRequest);
        ensureGreen(testIndex2);

        // create an api key to provoke a .security-N index
        Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "some-api-key"
            }""");
        client().performRequest(createApiKeyRequest);

        // fire off an async search to create an .async-search index
        Request asyncSearchRequest = new Request("POST", "/test_index_1/_async_search");
        asyncSearchRequest.addParameter("keep_on_completion", "true");
        asyncSearchRequest.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        String asyncSearchId;
        {
            Response response = client().performRequest(asyncSearchRequest);
            var map = responseAsMap(response);
            asyncSearchId = (String) map.get("id");
        }

        // verify that the async search exists and has the expected number of hits
        {
            Request getAsyncSearchRequest = new Request("GET", "/_async_search/" + asyncSearchId);
            Response response = client().performRequest(getAsyncSearchRequest);
            assertOK(response);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);

            int hits = objectPath.evaluate("response.hits.total.value");
            assertThat(hits, equalTo(numDocs));
        }

        // refresh all indices
        client().performRequest(new Request("POST", "/_refresh"));

        // get indices and check to make sure everything we're expecting to be there is there
        {
            Request getIndicesRequest = new Request("GET", "/*");
            getIndicesRequest.addParameter("expand_wildcards", "all");
            getIndicesRequest.setOptions(getIndicesRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            {
                Response response = client().performRequest(getIndicesRequest);
                var indices = responseAsMap(response).keySet();
                assertThat(indices, containsInAnyOrder(testIndex1, testIndex2, ".async-search", ".security-7", ".tasks"));
            }
        }

        // create a snapshot repository
        Request createRepositoryRequest = new Request("PUT", "/_snapshot/backup");
        createRepositoryRequest.setJsonEntity("""
            {
              "type": "fs",
              "settings": {
                "location": "backup_fs_location"
              }
            }""");
        client().performRequest(createRepositoryRequest);

        // take two snapshots
        for (String snapshot : List.of("test_snapshot_1", "test_snapshot_2")) {
            Request createSnapshotRequest = new Request("PUT", "/_snapshot/backup/" + snapshot);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            client().performRequest(createSnapshotRequest);
        }

        // create an index that won't allocate
        String testIndex3 = "test_index_3";
        {
            Request createIndexRequest = new Request("PUT", "/" + testIndex3);
            createIndexRequest.addParameter("wait_for_active_shards", "0");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                    "index": {
                      "number_of_shards": 1,
                      "number_of_replicas": 0,
                      "routing.allocation.require._name": "some_non_existent_node"
                    }
                  }
                }""");
            client().performRequest(createIndexRequest);
        }

        // take a third *partial* snapshot (it will be partial because of the unallocated index)
        {
            Request createSnapshotRequest = new Request("PUT", "/_snapshot/backup/partial_snapshot");
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("""
                {
                  "partial": true
                }""");
            client().performRequest(createSnapshotRequest);
        }

        // manually delete an index to simulate some sort of data loss
        deleteIndex(testIndex1);

        // restore the project
        Request restoreProjectRequest = new Request("POST", "/_snapshot/backup/_latest_success/_restore_project");
        restoreProjectRequest.setOptions(restoreProjectRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        {
            Response response = client().performRequest(restoreProjectRequest);
            assertOK(response);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);

            String snapshot = objectPath.evaluate("snapshot.snapshot");
            assertThat(snapshot, equalTo("test_snapshot_2")); // note: not partial_snapshot

            List<String> indices = objectPath.evaluate("snapshot.indices");
            assertThat(indices, containsInAnyOrder(testIndex1, testIndex2, ".async-search", ".security-7", ".tasks"));

            int successful = objectPath.evaluate("snapshot.shards.successful");
            assertThat(successful, equalTo(6)); // 2 for test_index_1; 1 for test_index_2 and each of the 3 system indices

            int failed = objectPath.evaluate("snapshot.shards.failed");
            assertThat(failed, equalTo(0));
        }

        // get indices and check to make sure everything we're expecting to be there is there
        {
            Request getIndicesRequest = new Request("GET", "/*");
            getIndicesRequest.addParameter("expand_wildcards", "all");
            getIndicesRequest.setOptions(getIndicesRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            {
                Response response = client().performRequest(getIndicesRequest);
                var indices = responseAsMap(response).keySet();
                assertThat(indices, containsInAnyOrder(testIndex1, testIndex2, ".async-search", ".security-7", ".tasks"));

                // being more explicit than the previous assertThat -- the third index has been destroyed, it wasn't in the
                // most recent successful snapshot (test_snapshot_2), and _restore_project deletes everything and then restores
                assertThat(indices, not(contains(testIndex3)));
            }
        }

        // verify that the async search exists and has the expected number of hits
        ensureGreen(".async-search");
        {
            Request getAsyncSearchRequest = new Request("GET", "/_async_search/" + asyncSearchId);
            Response response = client().performRequest(getAsyncSearchRequest);
            assertOK(response);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);

            int hits = objectPath.evaluate("response.hits.total.value");
            assertThat(hits, equalTo(numDocs));
        }
    }
}
