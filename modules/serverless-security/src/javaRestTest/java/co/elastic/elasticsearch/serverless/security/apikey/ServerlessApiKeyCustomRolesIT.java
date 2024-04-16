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

package co.elastic.elasticsearch.serverless.security.apikey;

import co.elastic.elasticsearch.serverless.security.AbstractServerlessCustomRolesRestTestCase;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessApiKeyCustomRolesIT extends AbstractServerlessCustomRolesRestTestCase {

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

    public void testApiKeys() throws IOException {
        enableStrictValidation();
        doTestValidApiKey();
        doTestApiKeyWithWorkflowRestriction();
        doTestApiKeyWithEmptyRoleDescriptors();
        doTestApiKeyWithoutRoleDescriptors();

        doTestApiKeyWithCustomRoleValidationError();
        doTestApiKeyWithRoleParsingError();
        doTestApiKeyWithMixedValidAndInvalidRoles();
        doTestGrantApiKeyWithCustomRoleValidationError();
    }

    public void testApiKeysStrictValidationDisabled() throws IOException {
        disableStrictValidation();
        doTestValidApiKey();
        doTestApiKeyWithWorkflowRestriction();
        doTestApiKeyWithEmptyRoleDescriptors();
        doTestApiKeyWithoutRoleDescriptors();

        doTestApiKeyWithCustomRoleValidationErrorStrictValidationDisabled();
        doTestApiKeyWithRoleParsingErrorStrictValidationDisabled();
        doTestApiKeyWithMixedValidAndInvalidRoles();
    }

    private void doTestValidApiKey() throws IOException {
        final var payload = """
            {
              "superuser": {
                "cluster": ["all"]
              },
              "_underscored": {
                "cluster": ["none"]
              },
              "file_based_role": {
                "cluster": ["manage"]
              },
              "role-0": {
                "cluster": ["all"],
                "indices": [
                  {
                    "names": ["index-a", "*"],
                    "privileges": ["read"],
                    "query": "{\\"match\\":{\\"field\\":\\"a\\"}}",
                    "field_security" : {
                      "grant": ["field"]
                    }
                  }
                ],
                "applications": [
                  {
                    "application": "*",
                    "privileges": [ "*" ],
                    "resources": [ "*" ]
                  },
                  {
                    "application": "kibana-.kibana",
                    "privileges": [ "feature.read" ],
                    "resources": [ "*" ]
                  },
                  {
                    "application": "apm",
                    "privileges": [ "event:write", "config_agent:read" ],
                    "resources": [ "*" ]
                  }
                ],
                "metadata": {
                  "env": ["prod"]
                }
              }
            }""";
        executeApiKeyActionsAndAssertSuccess(TEST_USER, payload);
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER, payload);
    }

    private void doTestApiKeyWithWorkflowRestriction() throws IOException {
        final var payload = """
            {
              "role-0": {
                "restriction": {
                  "workflows": ["search_application_query"]
                },
                "metadata": {
                  "env": ["prod"]
                }
              }
            }""";
        executeApiKeyActionsAndAssertSuccess(TEST_USER, payload);
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER, payload);
    }

    private void doTestApiKeyWithEmptyRoleDescriptors() throws IOException {
        final var payload = """
            {}
            """;
        executeApiKeyActionsAndAssertSuccess(TEST_USER, payload);
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER, payload);
    }

    private void doTestApiKeyWithoutRoleDescriptors() throws IOException {
        executeApiKeyActionsAndAssertSuccess(TEST_USER);
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER);
    }

    private void doTestApiKeyWithCustomRoleValidationError() throws IOException {
        final var payload = """
            {
              "role-0": {
                "cluster": ["all", "manage_ilm"]
              }
            }""";
        executeApiKeyActionsAndAssertFailure(
            TEST_USER,
            payload,
            "cluster privilege [manage_ilm] exists but is not supported when running in serverless mode"
        );
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER, payload);
    }

    private void doTestApiKeyWithMixedValidAndInvalidRoles() {
        var payload = """
            {
              "role-0": {
                "cluster": ["all"]
              },
              "role-1": {
                "cluster": ["invalid_privilege"]
              }
            }""";
        executeApiKeyActionsAndAssertFailure(TEST_USER, payload, "unknown cluster privilege [invalid_privilege]");
        executeApiKeyActionsAndAssertFailure(TEST_OPERATOR_USER, payload, "unknown cluster privilege [invalid_privilege]");
    }

    private void doTestGrantApiKeyWithCustomRoleValidationError() throws IOException {
        final var payload = """
            {
              "role-0": {
                "cluster": ["all", "manage_ilm"]
              }
            }""";
        grantApiKeyAndAssertSuccess(TEST_USER, payload);
    }

    private void doTestApiKeyWithCustomRoleValidationErrorStrictValidationDisabled() throws IOException {
        disableStrictValidation();
        final var payload = """
            {
              "role-0": {
                "cluster": ["all", "manage_ilm"]
              }
            }""";
        executeApiKeyActionsAndAssertSuccess(TEST_USER, payload);
        executeApiKeyActionsAndAssertSuccess(TEST_OPERATOR_USER, payload);
    }

    private void doTestApiKeyWithRoleParsingError() {
        final var payload = """
            {
              "role-0": {
                "remote_indices": [
                  {
                    "names": ["*"],
                    "privileges": ["read"]
                  }
                ]
              }
            }""";
        executeApiKeyActionsAndAssertFailure(TEST_USER, payload, "field [remote_indices] is not supported when running in serverless mode");
        executeApiKeyActionsAndAssertFailure(
            TEST_OPERATOR_USER,
            payload,
            "failed to parse remote indices privileges for role [role-0]. missing required [clusters] field"
        );
    }

    private void doTestApiKeyWithRoleParsingErrorStrictValidationDisabled() throws IOException {
        disableStrictValidation();
        final var payload = """
            {
              "role-0": {
                "remote_indices": [
                  {
                    "names": ["*"],
                    "privileges": ["read"]
                  }
                ]
              }
            }""";
        executeApiKeyActionsAndAssertFailure(
            TEST_USER,
            payload,
            "failed to parse remote indices privileges for role [role-0]. missing required [clusters] field"
        );
        executeApiKeyActionsAndAssertFailure(
            TEST_OPERATOR_USER,
            payload,
            "failed to parse remote indices privileges for role [role-0]. missing required [clusters] field"
        );
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
            Settings.builder().put("xpack.security.authc.api_key.strict_request_validation.enabled", value).build()
        );
    }

    private void executeApiKeyActionsAndAssertSuccess(String username, String roleDescriptorsPayload) throws IOException {
        final String id = createApiKeyAndAssertSuccess(username, Strings.format("""
            {
              "name": "api-key-0",
              "role_descriptors": %s
            }
            """, roleDescriptorsPayload));
        updateApiKeyAndAssertSuccess(username, id, Strings.format("""
            {
              "role_descriptors": %s
            }
            """, roleDescriptorsPayload));
        bulkUpdateApiKeyAndAssertSuccess(username, Strings.format("""
            {
              "ids": ["%s"],
              "role_descriptors": %s
            }
            """, id, roleDescriptorsPayload));
        grantApiKeyAndAssertSuccess(username, roleDescriptorsPayload);
    }

    private void executeApiKeyActionsAndAssertSuccess(String username) throws IOException {
        final String id = createApiKeyAndAssertSuccess(username, """
            {
              "name": "api-key-0"
            }
            """);
        updateApiKeyAndAssertSuccess(username, id, """
            {}
            """);
        bulkUpdateApiKeyAndAssertSuccess(username, Strings.format("""
            {
              "ids": ["%s"]
            }
            """, id));
    }

    private String createApiKeyAndAssertSuccess(String username, String payload) throws IOException {
        final var request = new Request("POST", "/_security/api_key");
        request.setJsonEntity(payload);
        final Response response = executeAndAssertSuccess(username, request);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(response);
        return (String) createApiKeyResponseMap.get("id");
    }

    private String grantApiKeyAndAssertSuccess(String username, String roleDescriptorsPayload) throws IOException {
        final Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD)))
        );
        final String payload = Strings.format("""
            {
              "grant_type": "password",
              "username": "%s",
              "password": "%s",
              "api_key": {
                "name": "api-key-0",
                "role_descriptors": %s
              }
            }
            """, username, TEST_PASSWORD, roleDescriptorsPayload);
        request.setJsonEntity(payload);

        // Note: not a type - the API is internal and *must* be executed by the operator user
        final Response response = executeAndAssertSuccess(TEST_OPERATOR_USER, request);
        final Map<String, Object> responseBody = entityAsMap(response);
        return (String) responseBody.get("id");
    }

    private void updateApiKeyAndAssertSuccess(String username, String id, String payload) throws IOException {
        final var request = new Request("PUT", "/_security/api_key/" + id);
        request.setJsonEntity(payload);
        executeAndAssertSuccess(username, request);
    }

    private void bulkUpdateApiKeyAndAssertSuccess(String username, String payload) throws IOException {
        final var request = new Request("POST", "/_security/api_key/_bulk_update");
        request.setJsonEntity(payload);
        executeAndAssertSuccess(username, request);
    }

    private void executeApiKeyActionsAndAssertFailure(String username, String payload, String message) {
        createApiKeyAndAssertFailure(username, Strings.format("""
            {
              "name": "api-key-0",
              "role_descriptors": %s
            }
            """, payload), message);
        updateApiKeyAndAssertFailure(username, Strings.format("""
            {
              "role_descriptors": %s
            }
            """, payload), message);
        // The `ids` field just needs to be present; it doesn't matter what value it has since parsing or validation is expected to fail
        // _before_ we get to looking up the API key for the update
        bulkUpdateApiKeyAndAssertFailure(username, Strings.format("""
            {
              "ids": ["some-id"],
              "role_descriptors": %s
            }
            """, payload), message);
    }

    private void createApiKeyAndAssertFailure(String username, String payload, String message) {
        final var request = new Request("POST", "/_security/api_key");
        request.setJsonEntity(payload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(message));
    }

    private void updateApiKeyAndAssertFailure(String username, String payload, String message) {
        // The `id` field just needs to be present; it doesn't matter what value it has since parsing or validation is expected to fail
        // _before_ we get to looking up the API key for the update
        final var request = new Request("PUT", "/_security/api_key/some-id");
        request.setJsonEntity(payload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(message));
    }

    private void bulkUpdateApiKeyAndAssertFailure(String username, String payload, String message) {
        final var request = new Request("POST", "/_security/api_key/_bulk_update");
        request.setJsonEntity(payload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(message));
    }
}
