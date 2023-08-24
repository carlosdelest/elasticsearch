/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.e2e;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;

import java.io.IOException;

public class ServerlessE2eTest extends AbstractServerlessE2eTest {
    public static final String INDEX_NAME = "test-e2e-index";

    @AfterClass
    public static void cleanup() throws IOException {
        deleteIndex(INDEX_NAME);
    }

    public void testBasicIndexing() throws Exception {
        int docCount = randomIntBetween(10, 50);
        createIndex(INDEX_NAME);
        for (int i = 0; i < docCount; i++) {
            indexDocument(INDEX_NAME);
        }
        assertDocCount(client(), INDEX_NAME, docCount);
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        indexRequest.addParameter("refresh", "true");
        assertOK(client().performRequest(indexRequest));
    }

}
