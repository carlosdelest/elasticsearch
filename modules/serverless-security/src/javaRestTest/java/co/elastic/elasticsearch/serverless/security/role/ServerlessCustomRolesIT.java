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

package co.elastic.elasticsearch.serverless.security.role;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ServerlessCustomRolesIT extends ESRestTestCase {
    private static final String TEST_OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_USER = "elastic-user";
    private static final String TEST_PASSWORD = "elastic-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .settings(ServerlessCustomRolesIT::randomisedSettings)
        .user(TEST_OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(TEST_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return false;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static Map<String, String> randomisedSettings(LocalClusterSpec.LocalNodeSpec localNodeSpec) {
        Map<String, String> settings = new HashMap<>();
        if (randomBoolean()) {
            // Verify that the native realm can be explicitly disabled (which the k8s-controller does) even if native users are disabled
            settings.put("xpack.security.authc.realms.native.disabled_native.enabled", "false");
        }
        if (randomBoolean()) {
            // Explicitly disable native user mgt, rather than relying on the serverless default
            settings.put("xpack.security.authc.native_users.enabled", "false");
        }
        settings.put("xpack.security.authc.native_roles.enabled", "true");
        return settings;
    }

    public void testPutValidCustomRole() throws IOException {
        doTestValidCustomRole();
        doTestEmptyRunAsIsValid();
    }

    public void testPutInvalidCustomRole() throws IOException {
        doTestUnsupportedClusterPrivilege();
        doTestUnsupportedIndexPrivileges();
        doTestInvalidQueryFieldInIndexPrivilege();
        doTestInvalidApplicationPrivilegeName();
        doTestUnsupportedField();
        doTestMalformedUnsupportedField();
        doTestRunAsNotSupported();
        doTestMalformedRunAs();
        doTestRoleNameCannotMatchFileBasedRole();
        doTestRoleNameCannotMatchReservedRole();
    }

    public void testOtherRoleApisNotExposed() throws IOException {
        final var rolePayload = """
            {
              "cluster": ["all"]
            }""";
        executeAndAssertSuccess(TEST_USER, "custom_role", rolePayload);

        {
            // Get Role not exposed yet to regular users
            final var e = expectThrows(ResponseException.class, () -> getRole(TEST_USER, "custom_role"));
            assertThat(e.getMessage(), containsString("not available when running in serverless mode"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }

        {
            // Delete Role not exposed yet to regular users
            final var e = expectThrows(
                ResponseException.class,
                () -> executeAsUser(TEST_USER, new Request("DELETE", "/_security/role/" + "custom_role"))
            );
            assertThat(e.getMessage(), containsString("not available when running in serverless mode"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    private void doTestInvalidApplicationPrivilegeName() throws IOException {
        final var rolePayload = """
            {
              "applications": [
                {
                  "application": "kibana-.*",
                  "privileges": [ "*" ],
                  "resources": [ "*" ]
                }
              ]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "invalid application name [kibana-.*]. name must be wildcard [*] or one of [kibana-.kibana]"
        );
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestValidCustomRole() throws IOException {
        final var rolePayload = """
            {
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
                }
              ],
              "metadata": {
                "env": ["prod"]
              }
            }""";
        executeAndAssertSuccess(TEST_USER, "custom_role", rolePayload);
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestEmptyRunAsIsValid() throws IOException {
        final var rolePayload = """
            {
              "run_as": []
            }""";
        executeAndAssertSuccess(TEST_USER, "custom_role", rolePayload);
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestUnsupportedClusterPrivilege() throws IOException {
        final var rolePayload = """
            {
              "cluster": ["all", "manage_ilm"]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "cluster privilege [manage_ilm] exists but is not supported when running in serverless mode"
        );
        // Operator user still succeeds because we don't enforce custom role restrictions
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestUnsupportedIndexPrivileges() throws IOException {
        final var rolePayload = """
            {
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_cross_cluster"]
                }
              ]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "index privilege [read_cross_cluster] exists but is not supported when running in serverless mode"
        );
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestInvalidQueryFieldInIndexPrivilege() {
        final var rolePayload = """
            {
              "indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "query": "{ \\"unknown\\": {\\"\\"} }"
                }
              ]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "failed to parse field 'query' for indices ["
                + Strings.arrayToCommaDelimitedString(new String[] { "index-a", "*" })
                + "] at index privilege [0] of role descriptor"
        );
        executeAndAssertFailure(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            400,
            "failed to parse field 'query' for indices ["
                + Strings.arrayToCommaDelimitedString(new String[] { "index-a", "*" })
                + "] at index privilege [0] of role descriptor"
        );
    }

    private void doTestUnsupportedField() throws IOException {
        final var rolePayload = """
            {
              "remote_indices": [
                {
                  "clusters": ["*"],
                  "names": ["*"],
                  "privileges": ["read"]
                }
              ]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "field [remote_indices] is not supported when running in serverless mode"
        );
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestMalformedUnsupportedField() {
        final var rolePayload = """
            {
              "remote_indices": [
                {
                  "names": ["*"],
                  "privileges": ["read"]
                }
              ]
            }""";
        // We don't validate the malformed payload but fail early since remote_indices is not supported at all
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "field [remote_indices] is not supported when running in serverless mode"
        );
        // For an operator user we do validate the malformed request and fail
        executeAndAssertFailure(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            400,
            "failed to parse remote indices privileges for role [custom_role]. missing required [clusters] field"
        );
    }

    private void doTestMalformedRunAs() {
        final var rolePayload = """
            {
              "run_as": {"field": "other"}
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "failed to parse role [custom_role]. In serverless mode run_as must be absent or empty."
        );
        executeAndAssertFailure(TEST_OPERATOR_USER, "custom_role", rolePayload, 400, "could not parse [run_as] field.");
    }

    private void doTestRunAsNotSupported() throws IOException {
        final var rolePayload = """
            {
              "run_as": ["bob"]
            }""";
        executeAndAssertFailure(
            TEST_USER,
            "custom_role",
            rolePayload,
            400,
            "failed to parse role [custom_role]. In serverless mode run_as must be absent or empty."
        );
        executeAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestRoleNameCannotMatchFileBasedRole() throws IOException {
        final var rolePayload = """
            {
              "cluster": ["all"]
            }""";
        // role is defined in `roles.yml` file
        executeAndAssertFailure(TEST_USER, "file_based_role", rolePayload, 400, "Role [file_based_role] is reserved and may not be used");
        executeAndAssertSuccess(TEST_OPERATOR_USER, "file_based_role", rolePayload);
    }

    private void doTestRoleNameCannotMatchReservedRole() {
        final var rolePayload = """
            {
              "cluster": ["all"]
            }""";
        executeAndAssertFailure(TEST_USER, "superuser", rolePayload, 400, "Role [superuser] is reserved and may not be used");
        executeAndAssertFailure(TEST_OPERATOR_USER, "superuser", rolePayload, 400, "Role [superuser] is reserved and may not be used");
    }

    private void executeAndAssertSuccess(String username, String roleName, String rolePayload) throws IOException {
        final var putRoleRequest = new Request(randomFrom("PUT", "POST"), "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(rolePayload);
        executeAndAssertSuccess(username, putRoleRequest);
        // Use operator user since route is not exposed yet
        assertThat(getRole(TEST_OPERATOR_USER, roleName), is(notNullValue()));
    }

    private void executeAndAssertFailure(String username, String roleName, String rolePayload, int statusCode, String message) {
        final var putRoleRequest = new Request(randomFrom("PUT", "POST"), "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(rolePayload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, putRoleRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        assertThat(e.getMessage(), containsString(message));
    }

    private void executeAndAssertSuccess(String username, Request putRoleRequest) throws IOException {
        final Response putRoleResponse = executeAsUser(username, putRoleRequest);
        assertOK(putRoleResponse);
    }

    private Response executeAsUser(String username, Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString(TEST_PASSWORD.toCharArray())))
        );
        return client().performRequest(request);
    }

    private RoleDescriptor getRole(String username, String roleName) throws IOException {
        final var request = new Request("GET", "/_security/role/" + roleName);
        final Response response = executeAsUser(username, request);
        assertOK(response);
        final XContentParser parser = responseAsParser(response);
        // skip name and surrounding tokens
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        return RoleDescriptor.parse(roleName, parser, false);
    }
}
