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

package co.elastic.elasticsearch.serverless.ml;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;

public class ClassificationIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "x_pack_rest_user";
    private static final String OPERATOR_PASSWORD = "x-pack-test-password";
    private static final String NOT_OPERATOR_USER = "not_operator";
    private static final String NOT_OPERATOR_PASSWORD = "not_operator_password";

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.security.operator_privileges.enabled", "true")
        .user(OPERATOR_USER, OPERATOR_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(NOT_OPERATOR_USER, NOT_OPERATOR_PASSWORD, User.ROOT_USER_ROLE, false)
        .withNode(mlNodeSpec -> mlNodeSpec.setting("node.roles", "[ml]"))
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(NOT_OPERATOR_USER, new SecureString(NOT_OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, new SecureString(OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testClassification() throws Exception {
        try (var client = client()) {
            {
                Request putSourceIndexRequest = new Request("PUT", "my-source");
                putSourceIndexRequest.setJsonEntity("""
                    {
                      "mappings": {
                        "properties": {
                          "x": {
                            "type": "integer"
                          },
                          "foo": {
                            "type": "keyword"
                          }
                        }
                      }
                    }""");
                Response putSourceIndexResponse = client.performRequest(putSourceIndexRequest);
                assertThat(EntityUtils.toString(putSourceIndexResponse.getEntity()), containsString("\"acknowledged\":true"));
            }
            {
                Request indexRequest = new Request("POST", "my-source/_doc");
                indexRequest.setJsonEntity("""
                    {
                      "x": -1,
                      "foo": "cat"
                    }""");
                for (int i = 0; i < 10; ++i) {
                    Response indexResponse = client.performRequest(indexRequest);
                    assertThat(EntityUtils.toString(indexResponse.getEntity()), containsString("\"result\":\"created\""));
                }
            }
            {
                Request indexRequest = new Request("POST", "my-source/_doc");
                indexRequest.setJsonEntity("""
                    {
                      "x": 1,
                      "foo": "dog"
                    }""");
                for (int i = 0; i < 10; ++i) {
                    Response indexResponse = client.performRequest(indexRequest);
                    assertThat(EntityUtils.toString(indexResponse.getEntity()), containsString("\"result\":\"created\""));
                }
            }
            {
                Request refreshRequest = new Request("POST", "my-source/_refresh");
                client.performRequest(refreshRequest);
            }
            {
                Request putAnalyticsRequest = new Request("PUT", "_ml/data_frame/analytics/my-analytics");
                putAnalyticsRequest.setJsonEntity("""
                    {
                      "source": {
                        "index": "my-source"
                      },
                      "dest": {
                        "index": "my-dest"
                      },
                      "analysis": {
                        "classification": {
                          "dependent_variable": "foo"
                        }
                      }
                    }""");
                Response putAnalyticsResponse = client.performRequest(putAnalyticsRequest);
                assertThat(EntityUtils.toString(putAnalyticsResponse.getEntity()), containsString("\"id\":\"my-analytics\""));
            }
            {
                Request startAnalyticsRequest = new Request("POST", "_ml/data_frame/analytics/my-analytics/_start");
                Response startAnalyticsResponse = client.performRequest(startAnalyticsRequest);
                assertThat(EntityUtils.toString(startAnalyticsResponse.getEntity()), containsString("\"acknowledged\":true"));
            }
            assertBusy(() -> {
                Request getAnalyticsStatsRequest = new Request("GET", "_ml/data_frame/analytics/my-analytics/_stats");
                Response getAnalyticsStatsResponse = client.performRequest(getAnalyticsStatsRequest);
                assertThat(EntityUtils.toString(getAnalyticsStatsResponse.getEntity()), containsString("\"state\":\"stopped\""));
            }, 30, TimeUnit.SECONDS);
            assertThat(indexExists("my-dest"), is(true));
        }
    }
}
