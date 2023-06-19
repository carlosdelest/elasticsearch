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

package co.elasticsearch.serverless.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ServerlessRestMainActionTests extends ESTestCase {

    public void testHeadResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build build = Build.CURRENT;

        final ServerlessMainResponse mainResponse = new ServerlessMainResponse(nodeName, clusterName, clusterUUID, build);
        XContentBuilder builder = JsonXContent.contentBuilder();
        RestRequest restRequest = new FakeRestRequest() {
            @Override
            public Method method() {
                return Method.HEAD;
            }
        };

        RestResponse response = ServerlessRestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));

        // the empty responses are handled in the HTTP layer so we do
        // not assert on them here
    }

    public void testGetResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build build = Build.CURRENT;
        final boolean prettyPrint = randomBoolean();

        final ServerlessMainResponse mainResponse = new ServerlessMainResponse(nodeName, clusterName, clusterUUID, build);
        XContentBuilder builder = JsonXContent.contentBuilder();

        Map<String, String> params = new HashMap<>();
        if (prettyPrint == false) {
            params.put("pretty", String.valueOf(prettyPrint));
        }
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        RestResponse response = ServerlessRestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.content().length(), greaterThan(0));

        XContentBuilder responseBuilder = JsonXContent.contentBuilder();
        if (prettyPrint) {
            // do this to mimic what the rest layer does
            responseBuilder.prettyPrint().lfAtEnd();
        }
        mainResponse.toXContent(responseBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference xcontentBytes = BytesReference.bytes(responseBuilder);
        assertEquals(xcontentBytes, response.content());
    }
}
