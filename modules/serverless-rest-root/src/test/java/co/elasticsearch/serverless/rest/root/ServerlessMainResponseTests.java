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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

public class ServerlessMainResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build current = Build.CURRENT;
        Build build = new Build(current.type(), current.hash(), current.date(), current.isSnapshot(), current.qualifiedVersion());

        ServerlessMainResponse response = new ServerlessMainResponse("nodeName", new ClusterName("clusterName"), clusterUUID, build);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(XContentHelper.stripWhitespace(Strings.format("""
            {
                "name": "nodeName",
                "cluster_name": "clusterName",
                "cluster_uuid": "%s",
                "version": {
                                "build_flavor": "serverless"
                            },
                "tagline": "You Know, for Search"
            }
            """, clusterUUID, current.hash())), Strings.toString(builder));
    }

    public void testToXContentNonOperator() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build current = Build.CURRENT;
        Build build = new Build(current.type(), current.hash(), current.date(), current.isSnapshot(), current.qualifiedVersion());

        ServerlessMainResponse response = new ServerlessMainResponse("nodeName", new ClusterName("clusterName"), clusterUUID, build);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.MapParams params = new ToXContent.MapParams(
            Map.of(RestRequest.RESPONSE_RESTRICTED, ServerlessScope.SERVERLESS_RESTRICTION)
        );
        response.toXContent(builder, params);

        assertEquals(XContentHelper.stripWhitespace(Strings.format("""
            {
                "version":{"build_flavor":"serverless"},
                "tagline": "You Know, for Search"
            }
            """, clusterUUID, current.hash())), Strings.toString(builder));
    }
}
