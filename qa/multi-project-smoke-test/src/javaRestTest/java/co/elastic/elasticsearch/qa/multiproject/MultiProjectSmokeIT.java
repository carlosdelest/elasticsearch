/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.multiproject;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.MultiProjectTestHelper.ProjectClient;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessMultiProjectRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

public class MultiProjectSmokeIT extends ServerlessMultiProjectRestTestCase {

    private static final String ADMIN_USERNAME = "admin-user";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    public static final TemporaryFolder CONFIG_DIR = new TemporaryFolder();

    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "true")
        .user(ADMIN_USERNAME, ADMIN_PASSWORD)
        .setting("xpack.watcher.enabled", "false")
        .setting("serverless.multi_project.enabled", "true")
        .node(0, nodeSpecBuilder -> nodeSpecBuilder.withConfigDir(() -> CONFIG_DIR.getRoot().toPath()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(CONFIG_DIR).around(cluster);

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected TemporaryFolder configDir() {
        return CONFIG_DIR;
    }

    @Override
    protected void beforeRemoveProjects() throws IOException {
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
    }

    @Override
    protected void assertProjects(RestClient client, List<String> projectIds) throws IOException {
        assertProjectIds(client, projectIds);
        // The default project does not have any project settings since it does not have any file settings
        assertProjectSettings(client, projectIds.stream().filter(id -> ProjectId.DEFAULT.id().equals(id) == false).toList());
    }

    private static void assertProjectSettings(RestClient client, List<String> projectIds) throws IOException {
        assertProjectSettings(client, projectIds, false);
    }

    private static void assertProjectSettings(RestClient client, List<String> projectIds, boolean geoipDownloaderEnabledForActiveProject)
        throws IOException {
        Request request = new Request("GET", "/_cluster/state?multi_project=true");
        ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
        List<Map<String, Object>> projectsSettingsList = response.evaluate("projects_registry.projects");

        Map<String, Map<String, Object>> projectsSettings = new HashMap<>();
        for (Map<String, Object> projectSettings : projectsSettingsList) {
            String id = (String) projectSettings.get("id");
            @SuppressWarnings("unchecked")
            Map<String, Object> settings = (Map<String, Object>) projectSettings.get("settings");
            projectsSettings.put(id, settings);
        }
        for (String projectId : projectIds) {
            assertThat(projectsSettings.get(projectId), is(notNullValue()));
            assertThat(
                projectsSettings.get(projectId).get("ingest.geoip.downloader.enabled"),
                equalTo(projectId.equals(activeProject) ? String.valueOf(geoipDownloaderEnabledForActiveProject) : "false")
            );
            assertThat(projectsSettings.get(projectId).get("stateless.object_store.type"), equalTo("fs"));
            assertThat(projectsSettings.get(projectId).get("stateless.object_store.bucket"), equalTo("project_" + projectId));
            assertThat(projectsSettings.get(projectId).get("stateless.object_store.base_path"), equalTo("base_path"));
            assertThat(projectsSettings.get(projectId).get("stateless.object_store.client"), equalTo("default"));
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return clientSettings(true);
    }

    @Override
    protected Settings restAdminSettings() {
        return clientSettings(false);
    }

    private Settings clientSettings(boolean projectScoped) {
        return clientSettings(projectScoped, activeProject);
    }

    private Settings clientSettings(boolean projectScoped, String projectId) {
        assertThat(projectId, notNullValue());
        String token = basicAuthHeaderValue(ADMIN_USERNAME, new SecureString(ADMIN_PASSWORD.toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        }
        return builder.build();
    }

    public void testBasicIndexOperationsWithOneProject() throws Exception {
        final var projectClient = projectClient(activeProject);
        final var indexName = randomIdentifier();

        if (randomBoolean()) {
            assertOK(projectClient.performRequest(new Request("PUT", "/" + indexName)));
        }

        // Index a document
        final Request indexRequest = new Request("POST", "/" + indexName + "/_doc");
        indexRequest.setJsonEntity("""
            { "field": "value" }
            """);
        indexRequest.addParameter("refresh", "true");
        final ObjectPath indexResponse = assertOKAndCreateObjectPath(projectClient.performRequest(indexRequest));

        // Get the document
        final String docId = indexResponse.evaluate("_id");
        final ObjectPath getResponse = assertOKAndCreateObjectPath(
            projectClient.performRequest(new Request("GET", "/" + indexName + "/_doc/" + docId))
        );
        assertThat(getResponse.evaluate("_source"), equalTo(Map.of("field", "value")));

        // Search the document
        final ObjectPath searchResponse = assertOKAndCreateObjectPath(projectClient.performRequest(new Request("GET", "/_search")));
        assertThat(searchResponse.evaluate("hits.total.value"), equalTo(1));
        assertThat(searchResponse.evaluate("hits.hits.0._id"), equalTo(docId));
    }

    public void testProjectStatusAPI() throws Exception {
        List<String> existingProjects = CollectionUtils.concatLists(List.of(activeProject), extraProjects);
        {
            var existingProjectId = randomFrom(existingProjects);
            var resp = getProjectStatus(existingProjectId);
            assertOK(resp);
            var projectStatusResponse = ObjectPath.createFromResponse(resp);
            assertThat(projectStatusResponse.evaluate("project_id"), equalTo(existingProjectId));
        }
        {
            String newProjectId = randomValueOtherThanMany(existingProjects::contains, ESTestCase::randomIdentifier);
            var responseException = expectThrows(ResponseException.class, () -> getProjectStatus(newProjectId));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
            putProject(newProjectId);
            assertBusy(() -> {
                try {
                    var resp = getProjectStatus(newProjectId);
                    assertOK(resp);
                    var projectStatusResponse = ObjectPath.createFromResponse(resp);
                    assertThat(projectStatusResponse.evaluate("project_id"), equalTo(newProjectId));
                } catch (ResponseException e) {
                    throw new AssertionError(e);
                }
            });
            removeProject(newProjectId);
            responseException = expectThrows(ResponseException.class, () -> getProjectStatus(newProjectId));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
        }
        {
            // invalid project ID
            var responseException = expectThrows(ResponseException.class, () -> getProjectStatus("****"));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        }
    }

    public void testConcurrentOperationsFromMultipleProjects() throws Exception {
        final List<Thread> threads = Stream.concat(Stream.of(activeProject), extraProjects.stream()).map(projectId -> new Thread(() -> {
            try {
                doTestForOneProjectClient(projectClient(projectId));
            } catch (IOException e) {
                fail(e, "failed for project: %s", projectId);
            }
        })).toList();

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(30000);
        }
        assertTrue(threads.stream().noneMatch(Thread::isAlive));
    }

    @FixForMultiProject(description = "Remove once ML autoscaling metrics work. See also https://elasticco.atlassian.net/browse/ES-10838")
    public void testAutoscalingAPIContainsNoError() throws IOException {
        final Request getAutoscalingMetricsRequest = new Request("GET", "/_internal/serverless/autoscaling");
        final Response response = adminClient().performRequest(getAutoscalingMetricsRequest);
        final ObjectPath objectPath = assertOKAndCreateObjectPath(response);
        assertThat(objectPath.evaluate("index.failure"), nullValue());
        assertThat(objectPath.evaluate("search.failure"), nullValue());
        assertThat(objectPath.evaluate("ml"), nullValue());
    }

    public void testUpdateProjectSettings() throws Exception {
        // Update the active project's project setting
        putProject(activeProject, Settings.builder().put("ingest.geoip.downloader.enabled", true).build());
        assertBusy(() -> assertProjectSettings(adminClient(), CollectionUtils.concatLists(List.of(activeProject), extraProjects), true));
    }

    private void doTestForOneProjectClient(ProjectClient projectClient) throws IOException {
        final String projectId = projectClient.getProjectId();
        final String index = "index";
        final String anotherIndex = projectId + "-index";
        final int numDocs = between(50, 200);

        logger.info("--> running test for project [{}] and indices [{},{}] with [{}] docs", projectId, index, anotherIndex, numDocs);
        try {
            assertOK(projectClient.performRequest(new Request("PUT", "/" + index)));
            assertOK(projectClient.performRequest(new Request("PUT", "/" + anotherIndex)));

            final Map<String, Integer> docCounts = new HashMap<>();
            final Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.addParameter("refresh", "true");
            StringBuilder bulkBody = new StringBuilder();
            for (int i = 0; i < numDocs; i++) {
                final String activeIndex = randomFrom(index, anotherIndex);
                bulkBody.append(Strings.format("""
                    { "index" : { "_index" : "%s" } }
                    { "field": "value-%s-%s-%s"}
                    """, activeIndex, projectId, activeIndex, i));
                docCounts.compute(activeIndex, (key, val) -> val == null ? 1 : val + 1);
            }
            bulkRequest.setJsonEntity(bulkBody.toString());
            assertOK(projectClient.performRequest(bulkRequest));

            final ObjectPath searchResponse = assertOKAndCreateObjectPath(
                projectClient.performRequest(new Request("GET", "/" + index + "," + anotherIndex + "/_search?size=" + numDocs))
            );

            assertThat("project " + projectId, searchResponse.evaluate("hits.total.value"), equalTo(numDocs));
            for (int i = 0; i < numDocs; i++) {
                final String indexName = searchResponse.evaluate("hits.hits." + i + "._index");
                assertThat(indexName, oneOf(index, anotherIndex));
                assertThat(docCounts.keySet(), hasItem(indexName));
                docCounts.put(indexName, docCounts.get(indexName) - 1);

                final Map<String, String> source = searchResponse.evaluate("hits.hits." + i + "._source");
                assertThat(source.get("field"), startsWith("value-" + projectId + "-" + indexName + "-"));
            }
            assertThat("docCounts: " + docCounts, docCounts.values(), everyItem(equalTo(0)));
        } finally {
            assertOK(projectClient.performRequest(new Request("DELETE", "/" + index + "*," + anotherIndex + "*")));
        }
    }

    private Response getProjectStatus(String projectId) throws Exception {
        return adminClient().performRequest(new Request("GET", "/_internal/serverless/project_status/" + projectId));
    }
}
