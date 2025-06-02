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

package co.elastic.elasticsearch.qa.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ProjectServiceAccountAuthIT extends ESRestTestCase {
    public static final Hasher HASHER = Hasher.resolve("ssha256");
    private static final String ADMIN_USERNAME = "admin-user";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";
    private static final Set<ProjectId> PROJECTS = Set.of(
        ProjectId.fromId("frehley"),
        ProjectId.fromId("criss"),
        ProjectId.fromId("stanley"),
        ProjectId.fromId("simmons")
    );

    private static final Map<ProjectId, ServiceAccountToken> KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT = PROJECTS.stream()
        .collect(
            Collectors.toMap(
                project -> project,
                project -> ServiceAccountToken.newToken(
                    ServiceAccount.ServiceAccountId.fromPrincipal("elastic/kibana"),
                    "test-kibana-token"
                )
            )
        );
    private static final Map<ProjectId, ServiceAccountToken> FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT = PROJECTS.stream()
        .collect(
            Collectors.toMap(
                project -> project,
                project -> ServiceAccountToken.newToken(
                    ServiceAccount.ServiceAccountId.fromPrincipal("elastic/fleet-server"),
                    "test-fleet-token"
                )
            )
        );
    private static final String PROJECT_JSON_TEMPLATE = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {}
        }""";

    private static final String SETTINGS_JSON_TEMPLATE = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
             }
        }""";

    private static final String SECRETS_JSON_TEMPLATE = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "project_secrets": {
                    "string_secrets": {
                        %s
                     }
                 }
             }
        }""";
    private static final AtomicInteger reservedStateVersionCounter = new AtomicInteger(1);
    private static final Map<ProjectId, MutableResource> mutableSecretsResourceByProjectId = new ConcurrentHashMap<>();
    private static final Map<ProjectId, MutableResource> mutableSettingsResourceByProjectId = new ConcurrentHashMap<>();
    private static final MutableResource mutableSettingsResource = MutableResource.from(
        Resource.fromString(
            Strings.format(SETTINGS_JSON_TEMPLATE, getProjectsStringArray(PROJECTS), reservedStateVersionCounter.incrementAndGet())
        )
    );

    @ClassRule
    public static ElasticsearchCluster cluster = initTestCluster();

    private static ElasticsearchCluster initTestCluster() {
        LocalClusterSpecBuilder<ServerlessElasticsearchCluster> specBuilder = ServerlessElasticsearchCluster.local()
            .setting("xpack.security.enabled", "true")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("serverless.multi_project.enabled", "true")
            .setting("health.node.enabled", "false")
            .setting("ingest.geoip.downloader.enabled", "false")
            .setting("ingest.geoip.downloader.eager.download", "false")
            .setting("metering.index-info-task.enabled", "false")
            .configFile("operator_users.yml", Resource.fromClasspath("operator_users.yml"))
            .user(ADMIN_USERNAME, ADMIN_PASSWORD, User.ROOT_USER_ROLE, false);

        PROJECTS.forEach(projectId -> {
            String secretsContent = getStringSecretsConfig(
                List.of(FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId), KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId))
            );
            MutableResource secretMutableResource = MutableResource.from(
                Resource.fromString(Strings.format(SECRETS_JSON_TEMPLATE, reservedStateVersionCounter.incrementAndGet(), secretsContent))
            );
            MutableResource settingsMutableResource = MutableResource.from(
                Resource.fromString(Strings.format(PROJECT_JSON_TEMPLATE, reservedStateVersionCounter.get()))
            );
            mutableSecretsResourceByProjectId.put(projectId, secretMutableResource);
            mutableSettingsResourceByProjectId.put(projectId, settingsMutableResource);
            specBuilder.configFile("operator/project-" + projectId + ".secrets.json", secretMutableResource)
                .configFile("operator/project-" + projectId + ".json", settingsMutableResource);
        });
        specBuilder.configFile("operator/settings.json", mutableSettingsResource);

        return specBuilder.build();
    }

    private static String getStringSecretsConfig(List<ServiceAccountToken> tokens) {
        return tokens.stream()
            .map(
                serviceAccountToken -> Strings.format(
                    "\"xpack.security.authc.project_service_token.elastic.%s.%s\":\"%s\"",
                    serviceAccountToken.getAccountId().serviceName(),
                    serviceAccountToken.getTokenName(),
                    hashPassword(serviceAccountToken.getSecret().toString())
                )
            )
            .collect(Collectors.joining(","));
    }

    private static String hashPassword(String password) {
        return new String(HASHER.hash(new SecureString(password.toCharArray())));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return clientSettings();
    }

    @Override
    protected Settings restAdminSettings() {
        return clientSettings();
    }

    private Settings clientSettings() {
        String token = basicAuthHeaderValue(ADMIN_USERNAME, new SecureString(ADMIN_PASSWORD.toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        return builder.build();
    }

    public void testAuthenticationSuccessful() throws Exception {
        var projectId = randomFrom(PROJECTS);
        final ServiceAccountToken serviceAccountToken = randomFrom(
            KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId),
            FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId)
        );
        serviceAccountAuthForProject(projectId, serviceAccountToken);
        assertCanAccessInternalApi(projectId, serviceAccountToken);
    }

    public void testAuthenticationFailUnknownProject() {
        var projectId = randomFrom(PROJECTS);
        var ex = expectThrows(
            ResponseException.class,
            () -> serviceAccountAuthForProject(
                randomUniqueProjectId(),
                randomFrom(KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId), FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId))
            )
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testAuthenticationFailWrongSecret() {
        var ex = expectThrows(
            ResponseException.class,
            () -> serviceAccountAuthForProject(
                randomFrom(PROJECTS),
                ServiceAccountToken.newToken(ServiceAccount.ServiceAccountId.fromPrincipal("elastic/fleet-server"), "test-fleet-token")
            )
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testAuthenticationFailMissingServiceAccount() {
        var ex = expectThrows(
            ResponseException.class,
            () -> serviceAccountAuthForProject(
                randomFrom(PROJECTS),
                ServiceAccountToken.newToken(
                    ServiceAccount.ServiceAccountId.fromPrincipal("not-so-elastic/fleet-server"),
                    "test-fleet-token"
                )
            )
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testUpdateTokenHash() throws Exception {
        var projectId = randomFrom(PROJECTS);
        var newKibanaToken = ServiceAccountToken.newToken(
            KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId).getAccountId(),
            KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId).getTokenName()
        );
        var newFleetToken = ServiceAccountToken.newToken(
            FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId).getAccountId(),
            FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId).getTokenName()
        );
        mutableSecretsResourceByProjectId.get(projectId)
            .update(
                Resource.fromString(
                    Strings.format(
                        SECRETS_JSON_TEMPLATE,
                        reservedStateVersionCounter.incrementAndGet(),
                        getStringSecretsConfig(List.of(newKibanaToken, newFleetToken))
                    )
                )
            );
        mutableSettingsResourceByProjectId.get(projectId)
            .update(Resource.fromString(Strings.format(PROJECT_JSON_TEMPLATE, reservedStateVersionCounter.get())));

        waitForReservedStateFileSettingsVersion(projectId, reservedStateVersionCounter.get());

        var ex = expectThrows(
            ResponseException.class,
            () -> serviceAccountAuthForProject(
                projectId,
                randomFrom(FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId), KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId))
            )
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
        serviceAccountAuthForProject(projectId, randomFrom(newFleetToken, newKibanaToken));

        var unchangedProjectId = randomValueOtherThan(projectId, () -> randomFrom(PROJECTS));
        serviceAccountAuthForProject(
            unchangedProjectId,
            randomFrom(
                KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(unchangedProjectId),
                FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(unchangedProjectId)
            )
        );
    }

    public void testRolesMapped() throws Exception {
        var projectId = randomFrom(PROJECTS);
        tryPrivileges(projectId, KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId));
    }

    private void tryPrivileges(ProjectId projectId, ServiceAccountToken serviceAccountToken) throws IOException {
        Request request = new Request("GET", "/_security/user/_has_privileges");
        configureRequestCredentials(request, projectId, serviceAccountToken);
        request.setJsonEntity("{\"cluster\": [\"manage_security\", \"manage_own_api_key\"]}");
        Response response = adminClient().performRequest(request);
        assertOK(response);
        var responseMap = responseAsMap(response);
        assertFalse(ObjectPath.evaluate(responseMap, "cluster.manage_security"));
        assertTrue(ObjectPath.evaluate(responseMap, "cluster.manage_own_api_key"));
    }

    private void waitForReservedStateFileSettingsVersion(ProjectId projectId, int version) throws Exception {
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "_cluster/state?multi_project=true"));
            var responseMap = responseAsMap(response);
            List<Map<String, Object>> projects = ObjectPath.evaluate(responseMap, "metadata.projects");

            for (var project : projects) {
                if (project.get("id").equals(projectId.id())) {
                    assertSame(ObjectPath.evaluate(project, "reserved_state.file_settings.version"), version);
                }
            }
        });
    }

    private void waitForReservedStateFileSettingsVersion(int version) throws Exception {
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "_cluster/state?multi_project=true"));
            var responseMap = responseAsMap(response);
            assertSame(ObjectPath.evaluate(responseMap, "metadata.reserved_state.file_settings.version"), version);
        });
    }

    @After
    public void resetProjectStates() throws Exception {
        PROJECTS.forEach(projectId -> {
            String secretsContent = getStringSecretsConfig(
                List.of(KIBANA_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId), FLEET_SERVICE_ACCOUNT_TOKEN_BY_PROJECT.get(projectId))
            );
            mutableSecretsResourceByProjectId.get(projectId)
                .update(
                    Resource.fromString(
                        Strings.format(SECRETS_JSON_TEMPLATE, reservedStateVersionCounter.incrementAndGet(), secretsContent)
                    )
                );
            mutableSettingsResourceByProjectId.get(projectId)
                .update(Resource.fromString(Strings.format(PROJECT_JSON_TEMPLATE, reservedStateVersionCounter.get())));
        });

        mutableSettingsResource.update(
            Resource.fromString(
                Strings.format(SETTINGS_JSON_TEMPLATE, getProjectsStringArray(PROJECTS), reservedStateVersionCounter.incrementAndGet())
            )
        );
        waitForReservedStateFileSettingsVersion(reservedStateVersionCounter.get());
    }

    private void serviceAccountAuthForProject(ProjectId projectId, ServiceAccountToken serviceAccountToken) throws Exception {
        final Request request = new Request("GET", "_security/_authenticate");
        configureRequestCredentials(request, projectId, serviceAccountToken);
        var resp = client().performRequest(request);
        assertOK(resp);
        var respMap = entityAsMap(resp);
        assertThat(respMap, notNullValue());
        assertThat(ObjectPath.evaluate(respMap, "token.name"), is(serviceAccountToken.getTokenName()));
        assertThat(ObjectPath.evaluate(respMap, "token.type"), is("_service_account_file"));
        assertThat(ObjectPath.evaluate(respMap, "operator"), is(true));
    }

    private void assertCanAccessInternalApi(ProjectId projectId, ServiceAccountToken serviceAccountToken) throws IOException {
        final var nodesRequest = new Request("GET", "_nodes");
        configureRequestCredentials(nodesRequest, projectId, serviceAccountToken);
        var nodesResp = client().performRequest(nodesRequest);
        assertOK(nodesResp);
    }

    private static void configureRequestCredentials(Request request, ProjectId projectId, ServiceAccountToken serviceAccountToken)
        throws IOException {
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("X-Elastic-Project-Id", projectId.id())
                .addHeader("Authorization", "Bearer " + serviceAccountToken.asBearerString().toString())
                .build()
        );
    }

    private static String getProjectsStringArray(Set<ProjectId> projectIds) {
        return projectIds.isEmpty() ? "[]" : projectIds.stream().map(ProjectId::id).collect(Collectors.joining("\",\"", "[\"", "\"]"));
    }

    @FixForMultiProject(description = "Enable when reset feature states work with multi-project")
    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    protected boolean shouldConfigureProjects() {
        return false;
    }
}
