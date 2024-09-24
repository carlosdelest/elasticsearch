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

package co.elastic.elasticsearch.metering.stats.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

abstract class MeteringStatsRestTestCase extends ESRestTestCase {

    int createAndLoadIndex(String indexName, Settings settings) throws IOException {
        if (randomBoolean()) {
            settings = Settings.builder().put(settings).put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();
        }
        createIndex(adminClient(), indexName, settings);
        int numDocs = randomIntBetween(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        adminClient().performRequest(bulkRequest);

        ensureGreen(adminClient(), indexName);

        adminClient().performRequest(new Request("POST", "/" + indexName + "/_flush"));
        return numDocs;
    }

    @SuppressWarnings("unchecked")
    int createAndLoadDatastreamWithRollover(
        String datastreamName,
        Map<String, Integer> indexNameToNumDocsMap,
        Map<String, String> datastreamIndexToDatastreamMap
    ) throws IOException {
        Request templateRequest = new Request("PUT", "/_index_template/" + datastreamName + "-template");
        String templateBody = Strings.format("""
            {
              "index_patterns": ["%s*"],
              "data_stream": { },
              "template": {
                "lifecycle": {
                  "data_retention": "7d"
                }
              }
            }
            """, datastreamName);
        templateRequest.setJsonEntity(templateBody);
        adminClient().performRequest(templateRequest);

        int numDocs1 = randomIntBetween(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs1; i++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\": \"2099-05-06T16:21:15.000Z\", \"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + datastreamName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        Map<String, Object> bulkResponse = entityAsMap(adminClient().performRequest(bulkRequest));
        List<Map<String, Map<String, Object>>> items = (List<Map<String, Map<String, Object>>>) bulkResponse.get("items");
        assertThat(items.size(), equalTo(numDocs1));
        for (Map<String, Map<String, Object>> item : items) {
            String indexName = (String) item.get("create").get("_index");
            indexNameToNumDocsMap.compute(indexName, (index, currentDocCount) -> currentDocCount == null ? 1 : currentDocCount + 1);
            datastreamIndexToDatastreamMap.put(indexName, datastreamName);
        }

        Request rolloverRequest = new Request("POST", "/" + datastreamName + "/_rollover");
        adminClient().performRequest(rolloverRequest);
        ensureGreen(adminClient(), datastreamName);

        int numDocs2 = randomIntBetween(1, 100);
        bulk = new StringBuilder();
        for (int i = 0; i < numDocs2; i++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\": \"2099-05-06T16:21:15.000Z\", \"foo\": \"bar\"}\n");
        }
        bulkRequest = new Request("POST", "/" + datastreamName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkResponse = entityAsMap(adminClient().performRequest(bulkRequest));
        items = (List<Map<String, Map<String, Object>>>) bulkResponse.get("items");
        for (Map<String, Map<String, Object>> item : items) {
            String indexName = (String) item.get("create").get("_index");
            indexNameToNumDocsMap.compute(indexName, (index, currentDocCount) -> currentDocCount == null ? 1 : currentDocCount + 1);
            datastreamIndexToDatastreamMap.put(indexName, datastreamName);
        }

        adminClient().performRequest(new Request("POST", "/" + datastreamName + "/_flush"));
        return numDocs1 + numDocs2;
    }
}
