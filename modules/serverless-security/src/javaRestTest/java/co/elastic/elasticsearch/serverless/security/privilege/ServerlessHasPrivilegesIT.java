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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
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
import java.util.List;

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
        if (randomBoolean()) {
            enableStrictValidation();
        } else {
            disableStrictValidation();
        }
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
        checkPrivilegesAndAssertExpectedResponse(TEST_USER, payload, expectedResponseTemplate);
        checkPrivilegesAndAssertExpectedResponse(TEST_OPERATOR_USER, payload, expectedResponseTemplate);
    }

    public void testInvalidHasPrivilegesChecks() throws IOException {
        if (randomBoolean()) {
            enableStrictValidation();
        } else {
            disableStrictValidation();
        }
        {
            final var payload = """
                {
                  "cluster": [ "manage_ilm", "manage", "delegate_pki" ]
                }""";
            final String expectedWarning = "HasPrivileges request includes privileges for features not available in serverless mode;"
                + " you will not have access to these features regardless of your permissions; "
                + "cluster privileges: [delegate_pki,manage_ilm];";
            checkPrivilegesAndAssertHeaderWarning(TEST_USER, payload, expectedWarning);
            checkPrivilegesAndAssertHeaderWarning(TEST_OPERATOR_USER, payload, expectedWarning);
        }

        {
            final var payload = """
                {
                  "index" : [
                    {
                      "names": [ "suppliers", "products" ],
                      "privileges": [ "read_cross_cluster", "manage_follow_index" ]
                    }
                  ]
                }""";
            final String expectedWarning = "HasPrivileges request includes privileges for features not available in serverless mode; "
                + "you will not have access to these features regardless of your permissions; "
                + "index privileges: [manage_follow_index,read_cross_cluster];";
            checkPrivilegesAndAssertHeaderWarning(TEST_USER, payload, expectedWarning);
            checkPrivilegesAndAssertHeaderWarning(TEST_OPERATOR_USER, payload, expectedWarning);
        }

        {
            final var payload = """
                {
                  "cluster": [ "manage_ilm", "manage", "delegate_pki" ],
                  "index" : [
                    {
                      "names": [ "suppliers", "products" ],
                      "privileges": [ "read_cross_cluster", "manage_follow_index" ]
                    }
                  ]
                }""";
            final String expectedWarning = "HasPrivileges request includes privileges for features not available in serverless mode; "
                + "you will not have access to these features regardless of your permissions; "
                + "cluster privileges: [delegate_pki,manage_ilm]; "
                + "index privileges: [manage_follow_index,read_cross_cluster];";
            checkPrivilegesAndAssertHeaderWarning(TEST_USER, payload, expectedWarning);
            checkPrivilegesAndAssertHeaderWarning(TEST_OPERATOR_USER, payload, expectedWarning);
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

    private void checkPrivilegesAndAssertHeaderWarning(String username, String payload, String message) throws IOException {
        final var hasPrivilegesRequest = new Request("POST", "/_security/user/_has_privileges");
        hasPrivilegesRequest.setJsonEntity(payload);
        hasPrivilegesRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> warnings.equals(List.of(message)) == false).build()
        );
        executeAndAssertSuccess(username, hasPrivilegesRequest);
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
