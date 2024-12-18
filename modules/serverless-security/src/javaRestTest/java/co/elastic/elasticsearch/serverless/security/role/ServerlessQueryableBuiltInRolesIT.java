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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class ServerlessQueryableBuiltInRolesIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "operator-user";
    private static final String TEST_USER = "security-user";
    private static final SecureString PASSWORD = new SecureString("x-pack-test-password".toCharArray());

    private static final String DEFAULT_FILE_ROLES = """
        editor:
          cluster: [ "manage_own_api_key" ]
          indices:
          - names:  [ "*" ]
            privileges:  [ "read" ]
          applications:
          - application: kibana-.kibana
            privileges: [ "all" ]
            resources: [ "*" ]
          metadata:
            _public: true
        viewer:
          cluster: [ "manage_own_api_key" ]
          indices:
          - names:  [ "*" ]
            privileges:  [ "read" ]
          applications:
          - application: kibana-.kibana
            privileges: [ "read" ]
            resources: [ "*" ]
          metadata:
            _public: true
        _operator_internal_only:
          cluster: [ "all" ]
          indices:
            - names: [ "*" ]
              privileges: [ "all" ]
          applications:
            - application: "*"
              privileges: [ "*" ]
              resources: [ "*" ]
        """;

    private static final MutableResource rolesFile = MutableResource.from(Resource.fromString(DEFAULT_FILE_ROLES));

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .nodes(2)
        .user(OPERATOR_USER, PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(TEST_USER, PASSWORD.toString(), User.ROOT_USER_ROLE, false)
        .rolesFile(rolesFile)
        .setting("xpack.ml.enabled", "false")
        .systemProperty("es.queryable_built_in_roles_enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testQueryBuiltInRoles() throws Exception {
        // have to update the roles file here, since the order of tests is not guaranteed
        rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES));
        final String[] builtInRoles = new String[] { "editor", "viewer" };
        assertBusy(() -> {
            assertQuery("", builtInRoles.length, roles -> {
                assertThat(roles, iterableWithSize(builtInRoles.length));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(builtInRoles)));
                }
            });
        });
    }

    public void testGetBuiltInRoles() throws Exception {
        // have to update the roles.yml file here, since the order of tests is not guaranteed
        rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES));

        // 1. Test get all roles as an operator user
        {
            final String[] builtInRoles = new String[] {
                "editor",
                "viewer",
                "superuser",
                "remote_monitoring_agent",
                "remote_monitoring_collector" };
            assertBusy(() -> {
                Request request = new Request("GET", "/_security/role");
                Response response = client().performRequest(request);
                assertOK(response);
                Map<String, ?> responseAsMap = responseAsMap(response);
                assertThat(responseAsMap.keySet(), containsInAnyOrder(builtInRoles));
            }, 10, TimeUnit.SECONDS);
        }

        // 2. Test get all roles as a regular user
        {
            final String[] builtInRoles = new String[] { "editor", "viewer" };
            assertBusy(() -> {
                Request request = new Request("GET", "/_security/role");
                request.setOptions(
                    RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(TEST_USER, PASSWORD)).build()
                );
                Response response = client().performRequest(request);
                assertOK(response);
                Map<String, ?> responseAsMap = responseAsMap(response);
                assertThat(responseAsMap.keySet(), containsInAnyOrder(builtInRoles));
            }, 10, TimeUnit.SECONDS);
        }
    }

    public void testDeleteBuiltInRole() throws Exception {
        // have to update the roles file here, since the order of tests is not guaranteed
        rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES));
        final String[] builtInRoles = new String[] { "editor", "viewer" };
        assertBusy(() -> {
            assertQuery("", builtInRoles.length, roles -> {
                assertThat(roles, iterableWithSize(builtInRoles.length));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(builtInRoles)));
                }
            });
        }, 10, TimeUnit.SECONDS);
        final String builtInRole = randomFrom(builtInRoles);
        var e = expectThrows(ResponseException.class, () -> deleteRole(builtInRole));
        assertThat(e.getMessage(), containsString("role [" + builtInRole + "] is reserved and cannot be deleted"));
    }

    public void testUpdateBuiltInRole() throws Exception {
        // have to update the roles file here, since the order of tests is not guaranteed
        rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES));
        final String[] builtInRoles = new String[] { "editor", "viewer" };
        assertBusy(() -> {
            assertQuery("", builtInRoles.length, roles -> {
                assertThat(roles, iterableWithSize(builtInRoles.length));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(builtInRoles)));
                }
            });
        }, 10, TimeUnit.SECONDS);
        final String builtInRole = randomFrom(builtInRoles);
        var e = expectThrows(ResponseException.class, () -> updateRole(builtInRole, """
            {"cluster":["all"]}"""));
        assertThat(e.getMessage(), containsString("Role [" + builtInRole + "] is reserved and may not be used"));
    }

    public void testRolesFileUpdateTriggersRolesSynchronization() throws Exception {
        // 1. Test adding a new 'admin' and 'developer` built-in roles to the roles.yml file
        {
            rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES + """
                developer:
                  cluster: [ "monitor", "manage_own_api_key" ]
                  indices:
                  - names:  [ "*" ]
                    privileges:  [ "read", "monitor" ]
                  applications:
                  - application: kibana-.kibana
                    privileges: [ "read" ]
                    resources: [ "*" ]
                  metadata:
                    _public: true
                admin:
                  cluster: [ "all" ]
                  indices:
                  - names: [ "*" ]
                    privileges: [ "all" ]
                  applications:
                  - application: "*"
                    privileges: [ "*" ]
                    resources: [ "*" ]
                  metadata:
                    _public: true"""));

            // The roles.yml file is reloaded every 5 seconds, so we need to wait at least 5s
            // for the reload to happen and synchronization of built-in roles to be completed.
            final String[] newBuiltInRoles = new String[] { "admin", "editor", "viewer", "developer" };
            assertBusy(() -> {
                assertQuery("", newBuiltInRoles.length, roles -> {
                    assertThat(roles, iterableWithSize(newBuiltInRoles.length));
                    for (var role : roles) {
                        assertThat((String) role.get("name"), is(oneOf(newBuiltInRoles)));
                    }
                });
            }, 10, TimeUnit.SECONDS);
        }

        // 2. Test updating the 'admin' built-in role in the roles.yml file
        {
            // first check that admin role does not have a description
            // and that application privileges are present
            assertQuery("""
                {"query":{"term":{"name":"admin"}}}""", 1, roles -> {
                assertThat(roles.get(0).get("description"), is(nullValue()));
                assertThat((List<?>) roles.get(0).get("applications"), is(iterableWithSize(1)));
            });

            // add a description field to the admin role and remove the application privileges
            rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES + """
                developer:
                  cluster: [ "monitor", "manage_own_api_key" ]
                  indices:
                  - names:  [ "*" ]
                    privileges:  [ "read", "monitor" ]
                  applications:
                  - application: kibana-.kibana
                    privileges: [ "read" ]
                    resources: [ "*" ]
                  metadata:
                    _public: true
                admin:
                  description: "This is the admin role"
                  cluster: [ "all" ]
                  indices:
                  - names: [ "*" ]
                    privileges: [ "all" ]
                  metadata:
                    _public: true"""));

            assertBusy(() -> {
                assertQuery("""
                    {"query":{"term":{"name":"admin"}}}""", 1, roles -> {
                    assertThat(roles, iterableWithSize(1));
                    assertThat((String) roles.get(0).get("description"), is("This is the admin role"));
                    assertThat((List<?>) roles.get(0).get("applications"), is(emptyIterable()));
                });
            }, 10, TimeUnit.SECONDS);
        }
        // 3. Test removing the 'admin' role from roles.yml file
        {
            rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES + """
                developer:
                  cluster: [ "monitor", "manage_own_api_key" ]
                  indices:
                  - names:  [ "*" ]
                    privileges:  [ "read", "monitor" ]
                  applications:
                  - application: kibana-.kibana
                    privileges: [ "read" ]
                    resources: [ "*" ]
                  metadata:
                    _public: true"""));
            final String[] newBuiltInRoles = new String[] { "editor", "viewer", "developer" };
            assertBusy(() -> {
                assertQuery("", newBuiltInRoles.length, roles -> {
                    assertThat(roles, iterableWithSize(newBuiltInRoles.length));
                    for (var role : roles) {
                        assertThat((String) role.get("name"), is(oneOf(newBuiltInRoles)));
                    }
                });
            }, 10, TimeUnit.SECONDS);
        }

        // 4. Test marking the 'developer' role as internal only
        {
            rolesFile.update(Resource.fromString(DEFAULT_FILE_ROLES + """
                developer:
                   cluster: [ "monitor", "manage_own_api_key" ]
                   indices:
                   - names:  [ "*" ]
                     privileges:  [ "read", "monitor" ]
                   applications:
                   - application: kibana-.kibana
                     privileges: [ "read" ]
                     resources: [ "*" ]
                   metadata:
                     _public: false"""));

            final String[] newBuiltInRoles = new String[] { "editor", "viewer" };
            assertBusy(() -> {
                assertQuery("", newBuiltInRoles.length, roles -> {
                    assertThat(roles, iterableWithSize(newBuiltInRoles.length));
                    for (var role : roles) {
                        assertThat((String) role.get("name"), is(oneOf(newBuiltInRoles)));
                    }
                });
            }, 10, TimeUnit.SECONDS);
        }

        // 5. Test removing all public built-in roles from the roles.yml file
        {
            rolesFile.update(Resource.fromString(randomBoolean() ? "" : """
                _operator_internal_only:
                  cluster: [ "all" ]
                  indices:
                    - names: [ "*" ]
                      privileges: [ "all" ]
                  applications:
                    - application: "*"
                      privileges: [ "*" ]
                      resources: [ "*" ]"""));

            assertBusy(() -> { assertQuery("", 0, roles -> { assertThat(roles, iterableWithSize(0)); }); }, 10, TimeUnit.SECONDS);
        }
    }

    private Response deleteRole(String role) throws IOException {
        Request request = new Request("DELETE", "/_security/role/" + role);
        return client().performRequest(request);
    }

    private Response updateRole(String role, String body) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + role);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private void assertQuery(String body, int total, Consumer<List<Map<String, Object>>> roleVerifier) throws IOException {
        assertQuery(client(), body, total, roleVerifier);
    }

    public static void assertQuery(RestClient client, String body, int total, Consumer<List<Map<String, Object>>> roleVerifier)
        throws IOException {
        Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
        request.setJsonEntity(body);
        Response response = client.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("total"), is(total));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> roles = new ArrayList<>((List<Map<String, Object>>) responseMap.get("roles"));
        assertThat("actual roles: " + roles, roles.size(), is(responseMap.get("count")));
        roleVerifier.accept(roles);
    }

}
