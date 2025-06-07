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

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.serverless.MultiProjectTestHelper.ProjectClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

public abstract class ServerlessMultiProjectRestTestCase extends ESRestTestCase {

    protected static String activeProject = null;
    protected static Set<String> extraProjects = null;
    protected static final AtomicLong reservedStateVersionCounter = new AtomicLong(0);

    protected MultiProjectTestHelper multiProjectTestHelper;

    @BeforeClass
    public static void randomizeProjectIds() {
        if (activeProject != null) {
            return;
        }
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 5, ESTestCase::randomIdentifier);
    }

    @Before
    public void configureProjects() throws Exception {
        initClient();
        multiProjectTestHelper = new MultiProjectTestHelper(configDir(), reservedStateVersionCounter);

        multiProjectTestHelper.putProject(adminClient(), activeProject, projectSettings(activeProject), projectSecrets(activeProject));
        assertProjectObjectStoreStarted(activeProject);
        for (var project : extraProjects) {
            multiProjectTestHelper.putProject(adminClient(), project, projectSettings(project), projectSecrets(project));
            assertProjectObjectStoreStarted(project);
        }

        // The admin client does not set a project id, and can see all projects
        assertBusy(
            () -> assertProjects(adminClient(), CollectionUtils.concatLists(List.of(ProjectId.DEFAULT.id(), activeProject), extraProjects))
        );

        // The test client can only see the project it targets
        assertProjects(client(), List.of(activeProject));
    }

    @After
    public void removeProjects() throws Exception {
        beforeRemoveProjects();
        for (var project : extraProjects) {
            multiProjectTestHelper.removeProject(adminClient(), project);
            assertProjectObjectStoreClosed(project);
        }
        multiProjectTestHelper.removeProject(adminClient(), activeProject);
        assertProjectObjectStoreClosed(activeProject);
    }

    @AfterClass
    public static void resetProjectIds() {
        activeProject = null;
        extraProjects = null;
    }

    protected abstract ElasticsearchCluster cluster();

    protected abstract TemporaryFolder configDir();

    protected Settings projectSettings(String projectId) {
        return Settings.builder().put("ingest.geoip.downloader.enabled", false).build();
    }

    protected Settings projectSecrets(String projectId) {
        return Settings.EMPTY;
    }

    protected void beforeRemoveProjects() throws IOException {}

    protected static List<String> getAllProjectIds() {
        assert activeProject != null;
        return Stream.concat(Stream.of(activeProject), extraProjects.stream()).toList();
    }

    protected ProjectClient projectClient(String projectId) {
        return MultiProjectTestHelper.projectClient(client(), projectId);
    }

    protected void putProject(String project) throws Exception {
        multiProjectTestHelper.putProject(adminClient(), project);
    }

    protected void putProject(String project, Settings projectSettings) throws Exception {
        multiProjectTestHelper.putProject(adminClient(), project, projectSettings);
    }

    protected void removeProject(String project) throws Exception {
        multiProjectTestHelper.removeProject(adminClient(), project);
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    protected void assertProjects(RestClient client, List<String> expectedProjects) throws IOException {}

    @FixForMultiProject(description = "consider removing it once the project object store is fully integrated and tested more directly")
    private void assertProjectObjectStoreStarted(String projectId) throws Exception {
        assertBusy(() -> {
            try (var serverLog = cluster().getNodeLog(between(0, 1), LogType.SERVER)) {
                final List<String> allLines = Streams.readAllLines(serverLog);
                assertThat(
                    allLines,
                    hasItem(
                        containsString(
                            "object store started for project [" + projectId + "], type [fs], bucket [project_" + projectId + "]"
                        )
                    )
                );
            }
        });
    }

    @FixForMultiProject(description = "consider removing it once the project object store is fully integrated and tested more directly")
    private void assertProjectObjectStoreClosed(String projectId) throws Exception {
        assertBusy(() -> {
            try (var serverLog = cluster().getNodeLog(between(0, 1), LogType.SERVER)) {
                final List<String> allLines = Streams.readAllLines(serverLog);
                assertThat(allLines, hasItem(containsString("object store closed for project [" + projectId + "]")));
            }
        });
    }
}
