/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.rest;

import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTask;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.MultiProjectTestHelper;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.notNullValue;

@FixForMultiProject(description = "shttps://elasticco.atlassian.net/browse/ES-10292")
public class ServerlessXpackRestIT extends AbstractXPackRestTest {

    public static final boolean MULTI_PROJECT_ENABLED = Boolean.parseBoolean(System.getProperty("tests.multi_project.enabled", "false"));

    public static final TemporaryFolder CONFIG_DIR = new TemporaryFolder();
    private static final AtomicLong reservedStateVersionCounter = new AtomicLong(0);

    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("yamlRestTest")
        .setting("serverless.multi_project.enabled", String.valueOf(MULTI_PROJECT_ENABLED))
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.profiling.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        // Integration tests are supposed to enable/disable exporters before/after each test
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.authc.native_users.enabled", "true")
        .setting("xpack.security.authc.native_role_mappings.enabled", "true")
        .setting("stateless.translog.flush.interval", "20ms")
        .setting("stateless.online.prewarming.enabled", "false")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user("x_pack_rest_user", "x-pack-test-password")
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("service_tokens", Resource.fromClasspath("service_tokens"))
        .withNode(mlNodeSpec -> mlNodeSpec.setting("node.roles", "[remote_cluster_client,ml,transform]"))
        .node(0, nodeSpecBuilder -> nodeSpecBuilder.withConfigDir(() -> CONFIG_DIR.getRoot().toPath()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(CONFIG_DIR).around(cluster);

    public ServerlessXpackRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Predicate<String> waitForPendingTasksFilter() {
        return super.waitForPendingTasksFilter().or(
            task -> task.contains(ObjectStoreGCTask.TASK_NAME) || task.contains("metering-index-info")
        );
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        Set<String> nodesVersions,
        TestFeatureService testFeatureService,
        Set<String> osList
    ) {
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            nodesVersions,
            testFeatureService,
            osList,
            (api, path) -> (api.getName().equals("ml.infer_trained_model") && path.deprecated()) == false
        );
    }

    @Override
    protected Map<String, String> getApiCallHeaders() {
        if (MULTI_PROJECT_ENABLED) {
            return Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, Metadata.DEFAULT_PROJECT_ID.id());
        } else {
            return super.getApiCallHeaders();
        }
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    private static String activeProject;
    private static Set<String> extraProjects;

    @BeforeClass
    public static void randomizeProjectIds() {
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 3, ESTestCase::randomIdentifier);
    }

    private static MultiProjectTestHelper multiProjectTestHelper;

    @Before
    public void configureProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        initClient();
        if (multiProjectTestHelper != null) {
            return;
        }
        multiProjectTestHelper = new MultiProjectTestHelper(CONFIG_DIR, reservedStateVersionCounter);

        multiProjectTestHelper.putProject(adminClient(), activeProject);
        for (var project : extraProjects) {
            multiProjectTestHelper.putProject(adminClient(), project);
        }

        // The admin client does not set a project id, and can see all projects
        assertBusy(
            () -> assertProjectIds(
                adminClient(),
                CollectionUtils.concatLists(List.of(ProjectId.DEFAULT.id(), activeProject), extraProjects)
            )
        );

        // The test client can only see the project it targets
        assertProjectIds(client(), List.of(activeProject));
    }

    @After
    public void ensureEmptyProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            assertEmptyProject(project);
        }
    }

    @Override
    protected Settings restClientSettings() {
        return clientSettings(MULTI_PROJECT_ENABLED);
    }

    @Override
    protected Settings restAdminSettings() {
        return clientSettings(false);
    }

    @Override
    protected Settings cleanupClientSettings() {
        return restClientSettings();
    }

    private Settings clientSettings(boolean projectScoped) {
        return clientSettings(projectScoped, activeProject);
    }

    private Settings clientSettings(boolean projectScoped, String projectId) {
        assertThat(projectId, notNullValue());
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        }
        return builder.build();
    }
}
