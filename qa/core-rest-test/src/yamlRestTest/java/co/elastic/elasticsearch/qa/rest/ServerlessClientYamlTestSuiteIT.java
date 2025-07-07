/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.MultiProjectTestHelper;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.notNullValue;

@FixForMultiProject(description = "https://elasticco.atlassian.net/browse/ES-10292")
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class ServerlessClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public static final boolean MULTI_PROJECT_ENABLED = Booleans.parseBoolean(System.getProperty("tests.multi_project.enabled", "false"));

    public static final TemporaryFolder CONFIG_DIR = new TemporaryFolder();
    private static final AtomicLong reservedStateVersionCounter = new AtomicLong(0);

    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("serverless.multi_project.enabled", String.valueOf(MULTI_PROJECT_ENABLED))
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("indices.disk.interval", "-1") // Disable IndexingDiskController to avoid scewing stats
        .user("admin-user", "x-pack-test-password")
        .node(0, nodeSpecBuilder -> nodeSpecBuilder.withConfigDir(() -> CONFIG_DIR.getRoot().toPath()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(CONFIG_DIR).around(cluster);

    public ServerlessClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
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
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        }
        return builder.build();
    }
}
