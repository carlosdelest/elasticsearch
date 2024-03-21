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

package co.elastic.elasticsearch.serverless.security.privilege;

import co.elastic.elasticsearch.serverless.security.AbstractServerlessCustomRolesRestTestCase;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessHasPrivilegesIT extends AbstractServerlessCustomRolesRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user(TEST_OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(TEST_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testValidHasPrivilegesChecks() throws IOException {
        final var payload = """
            {
              "cluster": [ "monitor", "manage", "cluster:foo/bar" ],
              "index" : [
                {
                  "names": [ "suppliers" ],
                  "privileges": [ "read" ]
                },
                {
                  "names": [ "products" ],
                  "privileges" : [ "indices:foo/bar" ]
                }
              ],
              "application": [
                {
                  "application": "inventory_manager",
                  "privileges" : [ "read", "data:write/inventory" ],
                  "resources" : [ "product/1852563" ]
                }
              ]
            }""";
        final String expectedResponseTemplate = """
            {
              "username": "%s",
              "has_all_requested": true,
              "cluster": {
                "monitor": true,
                "manage": true,
                "cluster:foo/bar": true
              },
              "index": {
                "suppliers": {
                  "read": true
                },
                "products": {
                  "indices:foo/bar": true
                }
              },
              "application" : {
                "inventory_manager" : {
                  "product/1852563" : {
                    "read": true,
                    "data:write/inventory": true
                  }
                }
              }
            }
            """;
        enableStrictValidation();
        checkPrivilegesAndAssertExpectedResponse(TEST_USER, payload, expectedResponseTemplate);
        checkPrivilegesAndAssertExpectedResponse(TEST_OPERATOR_USER, payload, expectedResponseTemplate);

        disableStrictValidation();
        checkPrivilegesAndAssertExpectedResponse(TEST_USER, payload, expectedResponseTemplate);
        checkPrivilegesAndAssertExpectedResponse(TEST_OPERATOR_USER, payload, expectedResponseTemplate);
    }

    public void testInvalidHasPrivilegesChecks() throws IOException {
        enableStrictValidation();
        {
            final var payload = """
                {
                  "cluster": [ "manage_ilm", "manage" ]
                }""";
            checkPrivilegesAndAssertFailure(
                TEST_USER,
                payload,
                "cluster privilege [manage_ilm] exists but is not supported when running in serverless mode"
            );
            checkPrivilegesAndAssertSuccess(TEST_OPERATOR_USER, payload);
        }

        {
            final var payload = """
                {
                  "index" : [
                    {
                      "names": [ "suppliers", "products" ],
                      "privileges": [ "read_cross_cluster" ]
                    }
                  ]
                }""";
            checkPrivilegesAndAssertFailure(
                TEST_USER,
                payload,
                "index privilege [read_cross_cluster] exists but is not supported when running in serverless mode"
            );
            checkPrivilegesAndAssertSuccess(TEST_OPERATOR_USER, payload);
        }

        {
            final var payload = """
                {
                  "cluster": [ "unknown" ]
                }""";
            checkPrivilegesAndAssertFailure(TEST_USER, payload, "unknown cluster privilege [unknown]");
            // can't test this for an operator user because this currently trips an assertion
        }

        {
            final var payload = """
                {
                  "index" : [
                    {
                      "names": [ "suppliers", "products" ],
                      "privileges": [ "unknown" ]
                    }
                  ]
                }""";
            checkPrivilegesAndAssertFailure(TEST_USER, payload, "unknown index privilege [unknown]");
            // can't test this for an operator user because this currently trips an assertion
        }
    }

    public void testInvalidHasPrivilegesChecksStrictValidationDisabled() throws IOException {
        disableStrictValidation();
        {
            final var payload = """
                {
                  "cluster": [ "manage_ilm", "manage" ]
                }""";
            checkPrivilegesAndAssertSuccess(TEST_USER, payload);
            checkPrivilegesAndAssertSuccess(TEST_OPERATOR_USER, payload);
        }

        {
            final var payload = """
                {
                  "index" : [
                    {
                      "names": [ "suppliers", "products" ],
                      "privileges": [ "read_cross_cluster" ]
                    }
                  ]
                }""";
            checkPrivilegesAndAssertSuccess(TEST_USER, payload);
            checkPrivilegesAndAssertSuccess(TEST_OPERATOR_USER, payload);
        }
    }

    private void checkPrivilegesAndAssertExpectedResponse(String username, String payload, String expectedResponseTemplate)
        throws IOException {
        assertThat(
            entityAsMap(checkPrivilegesAndAssertSuccess(username, payload)),
            equalTo(XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.format(expectedResponseTemplate, username), false))
        );
    }

    private Response checkPrivilegesAndAssertSuccess(String username, String payload) throws IOException {
        final String endpoint = randomBoolean() ? "/_security/user/_has_privileges" : "/_security/user/" + username + "/_has_privileges";
        if (randomBoolean()) {
            final var hasPrivilegesRequest = new Request(randomFrom("POST", "GET"), endpoint);
            hasPrivilegesRequest.setJsonEntity(payload);
            return executeAndAssertSuccess(username, hasPrivilegesRequest);
        } else {
            // also possible to pass as source param
            final var hasPrivilegesRequest = new Request("GET", endpoint);
            hasPrivilegesRequest.addParameter("source", payload);
            hasPrivilegesRequest.addParameter("source_content_type", "application/json");
            return executeAndAssertSuccess(username, hasPrivilegesRequest);
        }
    }

    private void checkPrivilegesAndAssertFailure(String username, String payload, String message) {
        final var hasPrivilegesRequest = new Request("POST", "/_security/user/_has_privileges");
        hasPrivilegesRequest.setJsonEntity(payload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, hasPrivilegesRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(message));
    }

    private void enableStrictValidation() throws IOException {
        setStrictValidation(true);
    }

    private void disableStrictValidation() throws IOException {
        setStrictValidation(false);
    }

    private void setStrictValidation(boolean value) throws IOException {
        updateClusterSettings(
            adminClient(),
            Settings.builder().put("xpack.security.authz.has_privileges.strict_request_validation.enabled", value).build()
        );
    }
}
