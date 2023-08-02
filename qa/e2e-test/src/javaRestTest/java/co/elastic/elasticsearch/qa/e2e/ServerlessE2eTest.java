/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.e2e;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

@org.junit.Ignore
public class ServerlessE2eTest extends AbstractServerlessE2eTest {

    public void testBasicIndexing() throws Exception {
        String index = "test";
        int docCount = randomIntBetween(10, 50);
        createIndex(index, Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0").build());
        for (int i = 0; i < docCount; i++) {
            indexDocument(index);
        }
        assertDocCount(client(), index, docCount);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        indexRequest.addParameter("refresh", "true");
        assertOK(client().performRequest(indexRequest));
    }

}
