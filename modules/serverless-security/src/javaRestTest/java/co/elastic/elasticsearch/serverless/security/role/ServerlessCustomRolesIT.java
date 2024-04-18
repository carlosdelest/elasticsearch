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

import co.elastic.elasticsearch.serverless.security.AbstractServerlessCustomRolesRestTestCase;
import co.elastic.elasticsearch.serverless.security.privilege.ServerlessSupportedPrivilegesRegistry;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServerlessCustomRolesIT extends AbstractServerlessCustomRolesRestTestCase {
    private static final List<String> RESERVED_ROLES = List.of("superuser", "remote_monitoring_agent", "remote_monitoring_collector");

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
        settings.put("xpack.security.reserved_roles.include", Strings.collectionToCommaDelimitedString(RESERVED_ROLES));
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
        doTestWorkflowRestrictionsNotSupportedForRegularRole();
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
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "invalid application name [kibana-.*]. name must be a wildcard [*] or "
                + "one of the supported application names [apm,fleet,kibana-.kibana]",
            "action_request_validation_exception"
        );
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
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
                },
                {
                  "application": "apm",
                  "privileges": [ "event:write", "config_agent:read" ],
                  "resources": [ "*" ]
                },
                {
                  "application": "fleet",
                  "privileges": [ "no-privileges" ],
                  "resources": [ "*" ]
                }
              ],
              "metadata": {
                "env": ["prod"]
              }
            }""";
        putRoleAndAssertSuccess(TEST_USER, "custom_role", rolePayload);
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);

        assertThat(deleteRole(randomFrom(TEST_USER, TEST_OPERATOR_USER), "custom_role"), is(true));
    }

    private void doTestEmptyRunAsIsValid() throws IOException {
        final var rolePayload = """
            {
              "run_as": []
            }""";
        putRoleAndAssertSuccess(TEST_USER, "custom_role", rolePayload);
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestUnsupportedClusterPrivilege() throws IOException {
        final var rolePayload = """
            {
              "cluster": ["all", "manage_ilm"]
            }""";
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "cluster privilege [manage_ilm] exists but is not supported when running in serverless mode",
            "action_request_validation_exception"
        );
        // Operator user still succeeds because we don't enforce custom role restrictions
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestWorkflowRestrictionsNotSupportedForRegularRole() {
        final var rolePayload = """
            {
              "restriction": {
                "workflows": ["search_application_query"]
              }
            }""";
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "failed to parse role [custom_role]. unexpected field [restriction]",
            "parse_exception"
        );
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            "failed to parse role [custom_role]. unexpected field [restriction]",
            "parse_exception"
        );
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
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "index privilege [read_cross_cluster] exists but is not supported when running in serverless mode",
            "action_request_validation_exception"
        );
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
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
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "failed to parse field 'query' for indices ["
                + Strings.arrayToCommaDelimitedString(new String[] { "index-a", "*" })
                + "] at index privilege [0] of role descriptor",
            "parse_exception"
        );
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            "failed to parse field 'query' for indices ["
                + Strings.arrayToCommaDelimitedString(new String[] { "index-a", "*" })
                + "] at index privilege [0] of role descriptor",
            "parse_exception"
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
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "field [remote_indices] is not supported when running in serverless mode",
            "parse_exception"
        );
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
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
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "field [remote_indices] is not supported when running in serverless mode",
            "parse_exception"
        );
        // For an operator user we do validate the malformed request and fail
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            "failed to parse remote indices privileges for role [custom_role]. missing required [clusters] field",
            "parse_exception"
        );
    }

    private void doTestMalformedRunAs() {
        final var rolePayload = """
            {
              "run_as": {"field": "other"}
            }""";
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "failed to parse role [custom_role]. In serverless mode run_as must be absent or empty.",
            "parse_exception"
        );
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            "custom_role",
            rolePayload,
            "could not parse [run_as] field.",
            "parse_exception"
        );
    }

    private void doTestRunAsNotSupported() throws IOException {
        final var rolePayload = """
            {
              "run_as": ["bob"]
            }""";
        putRoleAndAssertValidationException(
            TEST_USER,
            "custom_role",
            rolePayload,
            "failed to parse role [custom_role]. In serverless mode run_as must be absent or empty.",
            "parse_exception"
        );
        putRoleAndAssertSuccess(TEST_OPERATOR_USER, "custom_role", rolePayload);
    }

    private void doTestRoleNameCannotMatchFileBasedRole() {
        final var rolePayload = """
            {
              "cluster": ["all"]
            }""";
        // role is defined in `roles.yml` file
        final String roleName = "file_based_role";
        putRoleAndAssertValidationException(
            TEST_USER,
            roleName,
            rolePayload,
            "Role [file_based_role] is reserved and may not be used",
            "action_request_validation_exception"
        );
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            roleName,
            rolePayload,
            "Role [file_based_role] is reserved and may not be used",
            "action_request_validation_exception"
        );
    }

    private void doTestRoleNameCannotMatchReservedRole() {
        final var rolePayload = """
            {
              "cluster": ["all"]
            }""";
        putRoleAndAssertValidationException(
            TEST_USER,
            "superuser",
            rolePayload,
            "Role [superuser] is reserved and may not be used",
            "action_request_validation_exception"
        );
        putRoleAndAssertValidationException(
            TEST_OPERATOR_USER,
            "superuser",
            rolePayload,
            "Role [superuser] is reserved and may not be used",
            "action_request_validation_exception"
        );
    }

    public void testGetRoles() throws IOException {
        putRoleAndAssertSuccess(TEST_USER, "custom_role_1", """
            {
              "cluster": ["all"]
            }""");

        putRoleAndAssertSuccess(TEST_USER, "custom_role_2", """
            {
              "cluster": ["none"]
            }""");

        assertThat(
            expectThrows(ResponseException.class, () -> getRoles(TEST_USER, "missing_role")).getResponse().getStatusLine().getStatusCode(),
            equalTo(404)
        );
        assertThat(getRoles(TEST_USER, "custom_role_1", "custom_role_2"), containsInAnyOrder("custom_role_1", "custom_role_2"));
        assertThat(getRoles(TEST_USER), containsInAnyOrder("custom_role_1", "custom_role_2"));
        assertThat(getRoles(TEST_USER, "custom_role_1", randomFrom(RESERVED_ROLES)), containsInAnyOrder("custom_role_1"));
        assertThat(
            expectThrows(ResponseException.class, () -> getRoles(TEST_USER, randomFrom(RESERVED_ROLES))).getResponse()
                .getStatusLine()
                .getStatusCode(),
            equalTo(404)
        );

        assertThat(
            expectThrows(ResponseException.class, () -> getRoles(TEST_OPERATOR_USER, "missing_role")).getResponse()
                .getStatusLine()
                .getStatusCode(),
            equalTo(404)
        );
        assertThat(getRoles(TEST_OPERATOR_USER, "custom_role_1", "custom_role_2"), containsInAnyOrder("custom_role_1", "custom_role_2"));
        final List<String> allRoles = new ArrayList<>(List.of("custom_role_1", "custom_role_2"));
        allRoles.addAll(RESERVED_ROLES);
        assertThat(getRoles(TEST_OPERATOR_USER), containsInAnyOrder(allRoles.toArray(new String[0])));
        assertThat(getRoles(TEST_OPERATOR_USER, "custom_role_1", "superuser"), containsInAnyOrder("custom_role_1", "superuser"));
        final List<String> reservedRoles = randomNonEmptySubsetOf(RESERVED_ROLES);
        assertThat(
            getRoles(TEST_OPERATOR_USER, reservedRoles.toArray(new String[0])),
            containsInAnyOrder(reservedRoles.toArray(new String[0]))
        );
    }

    public void testDeleteReservedRoles() {
        final String roleName = randomFrom(RESERVED_ROLES);
        {
            ResponseException responseException = expectThrows(ResponseException.class, () -> deleteRole(TEST_USER, roleName));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(responseException.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
        }
        {
            ResponseException responseException = expectThrows(ResponseException.class, () -> deleteRole(TEST_OPERATOR_USER, roleName));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(responseException.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
        }
    }

    public void testDeleteFileBasedRoles() {
        // role is defined in `roles.yml` file
        final String roleName = "file_based_role";
        {
            ResponseException responseException = expectThrows(ResponseException.class, () -> deleteRole(TEST_USER, roleName));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(responseException.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
        }
        {
            ResponseException responseException = expectThrows(ResponseException.class, () -> deleteRole(TEST_USER, roleName));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(responseException.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetBuiltinPrivileges() throws IOException {
        {
            final Response response = executeAndAssertSuccess(TEST_USER, new Request("GET", "/_security/privilege/_builtin"));
            final Map<String, Object> responseAsMap = responseAsMap(response);
            assertThat(
                (Collection<String>) responseAsMap.get("cluster"),
                contains(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames().stream().sorted().toArray())
            );
            assertThat(
                (Collection<String>) responseAsMap.get("index"),
                contains(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames().stream().sorted().toArray())
            );
        }
        // Same for operator users
        {
            final Response response = executeAndAssertSuccess(TEST_OPERATOR_USER, new Request("GET", "/_security/privilege/_builtin"));
            final Map<String, Object> responseAsMap = responseAsMap(response);
            assertThat(
                (Collection<String>) responseAsMap.get("cluster"),
                contains(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames().stream().sorted().toArray())
            );
            assertThat(
                (Collection<String>) responseAsMap.get("index"),
                contains(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames().stream().sorted().toArray())
            );
        }
    }

    public void testAuthorizationDeniedMessages() throws IOException {
        final var createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "api-key",
              "role_descriptors": {
                "empty": {"cluster": []}
              }
            }
            """);
        final var apiKeyResponse = executeAndAssertSuccess(TEST_USER, createApiKeyRequest);

        final var unauthorizedRequest = new Request("GET", "/_ingest/pipeline");
        final ResponseException e = expectThrows(ResponseException.class, () -> {
            unauthorizedRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + responseAsMap(apiKeyResponse).get("encoded"))
            );
            client().performRequest(unauthorizedRequest);
        });
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            e.getMessage(),
            containsString("this action is granted by the cluster privileges [read_pipeline,manage_pipeline,manage,all]")
        );
    }

    private void putRoleAndAssertSuccess(String username, String roleName, String rolePayload) throws IOException {
        final var putRoleRequest = new Request(randomFrom("PUT", "POST"), "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(rolePayload);
        executeAndAssertSuccess(username, putRoleRequest);
        final RoleDescriptor actualRoleDescriptor = getRoleAsRoleDescriptor(username, roleName);
        assertThat(actualRoleDescriptor.getName(), equalTo(roleName));
        assertThat(
            actualRoleDescriptor,
            equalTo(
                RoleDescriptor.parserBuilder().build().parse(actualRoleDescriptor.getName(), new BytesArray(rolePayload), XContentType.JSON)
            )
        );
    }

    private void putRoleAndAssertValidationException(
        String username,
        String roleName,
        String rolePayload,
        String message,
        String errorType
    ) {
        final var putRoleRequest = new Request(randomFrom("PUT", "POST"), "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(rolePayload);
        final ResponseException e = expectThrows(ResponseException.class, () -> executeAsUser(username, putRoleRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(message));
        Map<String, Object> response = null;
        try {
            response = ESRestTestCase.entityAsMap(e.getResponse());
        } catch (IOException ex) {
            fail("Should be able to parse error response");
        }
        assertThat(ObjectPath.eval("error.type", response), is(errorType));
    }

    private boolean deleteRole(String username, String roleName) throws IOException {
        final Response response = executeAsUser(username, new Request("DELETE", "/_security/role/" + roleName));
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        return (Boolean) responseMap.get("found");
    }

    private RoleDescriptor getRoleAsRoleDescriptor(String username, String roleName) throws IOException {
        final var request = new Request("GET", "/_security/role/" + roleName);
        final Response response = executeAsUser(username, request);
        assertOK(response);
        final XContentParser parser = responseAsParser(response);
        // skip name and surrounding tokens
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        return RoleDescriptor.parserBuilder().build().parse(roleName, parser);
    }

    private Set<String> getRoles(String username, String... roleNames) throws IOException {
        final var request = new Request("GET", "/_security/role/" + Strings.arrayToCommaDelimitedString(roleNames));
        return responseAsMap(executeAndAssertSuccess(username, request)).keySet();
    }
}
