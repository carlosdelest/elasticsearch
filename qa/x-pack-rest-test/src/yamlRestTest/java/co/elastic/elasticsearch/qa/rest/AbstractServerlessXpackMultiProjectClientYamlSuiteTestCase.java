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

package co.elastic.elasticsearch.qa.rest;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.Set;

@FixForMultiProject(description = "shttps://elasticco.atlassian.net/browse/ES-10292")
public abstract class AbstractServerlessXpackMultiProjectClientYamlSuiteTestCase extends AbstractXPackRestTest {
    public static final boolean MULTI_PROJECT_ENABLED = Boolean.parseBoolean(System.getProperty("es.test.multi_project.enabled", "false"));

    private static String activeProject;
    private static Set<String> extraProjects;
    private static boolean projectsConfigured = false;

    public AbstractServerlessXpackMultiProjectClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @BeforeClass
    public static void initializeProjectIds() {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        // The active project-id is slightly longer, and has a fixed suffix so that it's easier to pick in error messages etc.
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));
    }

    @Before
    public void configureProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        if (projectsConfigured) {
            return;
        }
        projectsConfigured = true;
        initClient();
        createProject(activeProject);
        for (var project : extraProjects) {
            createProject(project);
        }

        // The admin client does not set a project id, and can see all projects
        assertProjectIds(
            adminClient(),
            CollectionUtils.concatLists(List.of(Metadata.DEFAULT_PROJECT_ID.id(), activeProject), extraProjects)
        );
        // The test client can only see the project it targets
        assertProjectIds(client(), List.of(activeProject));
    }

    @After
    public final void assertEmptyProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            assertEmptyProject(project);
        }
    }

    @Override
    protected RestClient getCleanupClient() {
        return MULTI_PROJECT_ENABLED ? client() : super.getCleanupClient();
    }

    @Override
    protected Settings restAdminSettings() {
        return clientSettings(false);
    }

    @Override
    protected Settings restClientSettings() {
        return clientSettings(true);
    }

    private Settings clientSettings(boolean projectScoped) {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (MULTI_PROJECT_ENABLED && projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, activeProject);
        }
        return builder.build();
    }
}
