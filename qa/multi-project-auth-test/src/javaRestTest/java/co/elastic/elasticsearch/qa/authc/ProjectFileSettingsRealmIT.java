/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.realmSettingPrefix;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ProjectFileSettingsRealmIT extends ESRestTestCase {
    public static final Hasher HASHER = Hasher.resolve("ssha256");
    private static final List<String> ADMIN_ROLES = List.of("_es_test_root", "superuser");
    private static final List<String> TESTING_INTERNAL_ROLES = List.of("security_tester");
    private static final String ADMIN_DEFAULT_PASSWORD = "admin-test-password";
    private static final String TESTING_INTERNAL_DEFAULT_PASSWORD = "testing-internal-test-password";
    private static final String MULTI_REALM_NAME = "multi-file";
    private static final String ADMIN_USERNAME = "admin-user";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";
    private static final String ROLES = """
        security_tester:
          cluster:
            - "read_security"
        """;
    private static final Set<ProjectId> INITIAL_ACTIVE_PROJECTS = Set.of(ProjectId.fromId("stanley"), ProjectId.fromId("simmons"));
    private static final Set<ProjectId> ADDITIONAL_PROJECTS = Set.of(ProjectId.fromId("frehley"), ProjectId.fromId("criss"));

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
                        "xpack.security.authc.realms.project_file_settings.multi-file.admin.password_hash": "%s",
                        "xpack.security.authc.realms.project_file_settings.multi-file.testing-internal.password_hash": "%s"
                     }
                 }
             }
        }""";
    private static final AtomicInteger reservedStateVersionCounter = new AtomicInteger(1);
    private static final Map<ProjectId, MutableResource> mutableSecretsResourceByProjectId = new ConcurrentHashMap<>();
    private static final Map<ProjectId, MutableResource> mutableSettingsResourceByProjectId = new ConcurrentHashMap<>();
    private static final MutableResource mutableSettingsResource = MutableResource.from(
        Resource.fromString(
            Strings.format(
                SETTINGS_JSON_TEMPLATE,
                getProjectsStringArray(INITIAL_ACTIVE_PROJECTS),
                reservedStateVersionCounter.incrementAndGet()
            )
        )
    );

    public static ElasticsearchCluster cluster = initTestCluster();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(cluster);

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
            .setting(realmSettingPrefix("project_file_settings") + MULTI_REALM_NAME + ".order", "0")
            .setting(realmSettingPrefix("project_file_settings") + MULTI_REALM_NAME + ".admin.roles", String.join(",", ADMIN_ROLES))
            .setting(
                realmSettingPrefix("project_file_settings") + MULTI_REALM_NAME + ".testing-internal.roles",
                String.join(",", TESTING_INTERNAL_ROLES)
            )
            .rolesFile(Resource.fromString(ROLES))
            .user(ADMIN_USERNAME, ADMIN_PASSWORD, User.ROOT_USER_ROLE, true);

        Stream.concat(INITIAL_ACTIVE_PROJECTS.stream(), ADDITIONAL_PROJECTS.stream()).forEach(projectId -> {
            MutableResource secretMutableResource = MutableResource.from(
                Resource.fromString(
                    Strings.format(
                        SECRETS_JSON_TEMPLATE,
                        reservedStateVersionCounter.incrementAndGet(),
                        hashPassword(ADMIN_DEFAULT_PASSWORD),
                        hashPassword(TESTING_INTERNAL_DEFAULT_PASSWORD)
                    )
                )
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
        fileAuthForProject(randomFrom(INITIAL_ACTIVE_PROJECTS), "admin", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES);
        fileAuthForProject(
            randomFrom(INITIAL_ACTIVE_PROJECTS),
            "testing-internal",
            TESTING_INTERNAL_DEFAULT_PASSWORD,
            TESTING_INTERNAL_ROLES
        );
    }

    public void testAuthenticationFailUnknownProject() {
        var ex = expectThrows(
            ResponseException.class,
            () -> fileAuthForProject(randomUniqueProjectId(), "admin", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES)
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testAuthenticationFailWrongPassword() {
        var ex = expectThrows(
            ResponseException.class,
            () -> fileAuthForProject(randomFrom(INITIAL_ACTIVE_PROJECTS), "admin", "wrong-password", ADMIN_ROLES)
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testAuthenticationFailWrongUser() {
        var ex = expectThrows(
            ResponseException.class,
            () -> fileAuthForProject(randomFrom(INITIAL_ACTIVE_PROJECTS), "wrong-user", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES)
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testChangePassword() throws Exception {
        String newAdminPasswordHash = hashPassword("new-admin-password");
        String newTestingInternalPasswordHash = hashPassword("new-testing-internal-password");

        ProjectId projectId = randomFrom(INITIAL_ACTIVE_PROJECTS);
        mutableSecretsResourceByProjectId.get(projectId)
            .update(
                Resource.fromString(
                    Strings.format(
                        SECRETS_JSON_TEMPLATE,
                        reservedStateVersionCounter.incrementAndGet(),
                        newAdminPasswordHash,
                        newTestingInternalPasswordHash
                    )
                )
            );
        mutableSettingsResourceByProjectId.get(projectId)
            .update(Resource.fromString(Strings.format(PROJECT_JSON_TEMPLATE, reservedStateVersionCounter.get())));

        waitForReservedStateFileSettingsVersion(projectId, reservedStateVersionCounter.get());

        var ex = expectThrows(ResponseException.class, () -> fileAuthForProject(projectId, "admin", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));

        fileAuthForProject(projectId, "admin", "new-admin-password", ADMIN_ROLES);
        fileAuthForProject(projectId, "testing-internal", "new-testing-internal-password", TESTING_INTERNAL_ROLES);
    }

    public void testAddProject() throws Exception {
        ProjectId projectId = randomFrom(ADDITIONAL_PROJECTS);
        {
            var ex = expectThrows(
                ResponseException.class,
                () -> fileAuthForProject(projectId, "admin", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES)
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
        }
        mutableSettingsResource.update(
            Resource.fromString(
                Strings.format(
                    SETTINGS_JSON_TEMPLATE,
                    getProjectsStringArray(Set.of(projectId)),
                    reservedStateVersionCounter.incrementAndGet()
                )
            )
        );

        // Auth until it passes or fail if it doesn't
        assertBusy(() -> {
            try {
                fileAuthForProject(projectId, "admin", ADMIN_DEFAULT_PASSWORD, ADMIN_ROLES);
            } catch (ResponseException responseException) {
                fail(responseException);
            }
        });

        fileAuthForProject(projectId, "testing-internal", TESTING_INTERNAL_DEFAULT_PASSWORD, TESTING_INTERNAL_ROLES);
    }

    public void testRolesMapped() throws Exception {
        tryReadingSecurity(randomFrom(INITIAL_ACTIVE_PROJECTS), "testing-internal", TESTING_INTERNAL_DEFAULT_PASSWORD);
    }

    private void tryReadingSecurity(ProjectId projectId, String username, String password) throws IOException {
        Request request = new Request("GET", "/_security/privilege/_builtin");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("X-Elastic-Project-Id", projectId == null ? "default" : projectId.id())
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString(password.toCharArray())))
                .build()
        );
        assertOK(adminClient().performRequest(request));
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
    private void resetProjectStates() throws Exception {
        Stream.concat(INITIAL_ACTIVE_PROJECTS.stream(), ADDITIONAL_PROJECTS.stream()).forEach(projectId -> {
            mutableSecretsResourceByProjectId.get(projectId)
                .update(
                    Resource.fromString(
                        Strings.format(
                            SECRETS_JSON_TEMPLATE,
                            reservedStateVersionCounter.incrementAndGet(),
                            hashPassword(ADMIN_DEFAULT_PASSWORD),
                            hashPassword(TESTING_INTERNAL_DEFAULT_PASSWORD)
                        )
                    )
                );
            mutableSettingsResourceByProjectId.get(projectId)
                .update(Resource.fromString(Strings.format(PROJECT_JSON_TEMPLATE, reservedStateVersionCounter.get())));
        });

        mutableSettingsResource.update(
            Resource.fromString(
                Strings.format(
                    SETTINGS_JSON_TEMPLATE,
                    getProjectsStringArray(INITIAL_ACTIVE_PROJECTS),
                    reservedStateVersionCounter.incrementAndGet()
                )
            )
        );
        waitForReservedStateFileSettingsVersion(reservedStateVersionCounter.get());
    }

    private void fileAuthForProject(@Nullable ProjectId projectId, String username, String password, List<String> roles) throws Exception {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("X-Elastic-Project-Id", projectId == null ? "default" : projectId.id())
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString(password.toCharArray())))
                .build()
        );

        var resp = entityAsMap(client().performRequest(request));
        assertThat(resp.get("username"), equalTo(username));
        assertThat(ObjectPath.evaluate(resp, "authentication_realm.name"), equalTo(MULTI_REALM_NAME));
        assertEquals(roles, ObjectPath.evaluate(resp, "roles"));
    }

    private static String getProjectsStringArray(Set<ProjectId> projectIds) {
        return projectIds.isEmpty() ? "[]" : projectIds.stream().map(ProjectId::id).collect(Collectors.joining("\",\"", "[\"", "\"]"));
    }

    @FixForMultiProject(description = "Enable when reset feature states work with multi-project")
    @Override
    protected boolean resetFeatureStates() {
        return false;
    }
}
