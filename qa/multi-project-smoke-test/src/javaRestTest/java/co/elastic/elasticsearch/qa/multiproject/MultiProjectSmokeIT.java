/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.multiproject;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

public class MultiProjectSmokeIT extends ESRestTestCase {

    private static final AtomicInteger RESERVED_STATE_VERSION_COUNTER = new AtomicInteger(1);
    private static final String SETTINGS_JSON_TEMPLATE = """
        {
             "projects": [%s],
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
             }
        }""";

    private static final MutableResource mutableSettings = MutableResource.from(
        Resource.fromString(
            Strings.format(SETTINGS_JSON_TEMPLATE, projectIdsToCommaSeparatedList(Set.of()), RESERVED_STATE_VERSION_COUNTER.get())
        )
    );

    private static String projectIdsToCommaSeparatedList(Set<String> projectIds) {
        return projectIds.stream().map(id -> '"' + id + '"').collect(Collectors.joining(","));
    }

    private static final String ADMIN_USERNAME = "admin-user";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static String activeProject = null;
    private static Set<String> extraProjects = null;
    private static final TemporaryFolder CONFIG_DIR = new TemporaryFolder();

    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "true")
        .user(ADMIN_USERNAME, ADMIN_PASSWORD)
        .setting("xpack.watcher.enabled", "false")
        .setting("serverless.multi_project.enabled", "true")
        .configFile("operator/settings.json", mutableSettings)
        .node(0, nodeSpecBuilder -> nodeSpecBuilder.withConfigDir(() -> CONFIG_DIR.getRoot().toPath()))
        .build();

    @ClassRule
    public static TestRule testRule = RuleChain.outerRule(CONFIG_DIR).around(cluster);

    @BeforeClass
    public static void randomizeProjectIds() {
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 5, ESTestCase::randomIdentifier);
    }

    private Set<String> provisionedProjects;
    private Map<String, ProjectClient> projectClients;

    @Before
    public void configureProjects() throws Exception {
        initClient();
        provisionedProjects = new HashSet<>();
        projectClients = new HashMap<>();
        projectClients.put(activeProject, createProjectAndClient(activeProject));
        assertProjectObjectStoreStarted(activeProject);
        for (var project : extraProjects) {
            projectClients.put(project, createProjectAndClient(project));
            assertProjectObjectStoreStarted(project);
        }

        // The admin client does not set a project id, and can see all projects
        assertBusy(
            () -> assertProjects(adminClient(), CollectionUtils.concatLists(List.of(ProjectId.DEFAULT.id(), activeProject), extraProjects))
        );

        // The test client can only see the project it targets
        assertProjects(client(), List.of(activeProject));
    }

    @FixForMultiProject(description = "consider removing it once the project object store is fully integrated and tested more directly")
    private void assertProjectObjectStoreStarted(String projectId) throws Exception {
        assertBusy(() -> {
            try (var serverLog = cluster.getNodeLog(between(0, 1), LogType.SERVER)) {
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
            try (var serverLog = cluster.getNodeLog(between(0, 1), LogType.SERVER)) {
                final List<String> allLines = Streams.readAllLines(serverLog);
                assertThat(allLines, hasItem(containsString("object store closed for project [" + projectId + "]")));
            }
        });
    }

    private void assertProjects(RestClient client, List<String> projectIds) throws IOException {
        assertProjectIds(client, projectIds);
        // The default project does not have any project settings since it does not have any file settings
        assertProjectSettings(client, projectIds.stream().filter(id -> ProjectId.DEFAULT.id().equals(id) == false).toList());
    }

    private static void assertProjectSettings(RestClient client, List<String> projectIds) throws IOException {
        assertProjectSettings(client, projectIds, false);
    }

    private static void assertProjectSettings(RestClient client, List<String> projectIds, boolean geoipDownloaderEnabledForActiveProject)
        throws IOException {
        Request request = new Request("GET", "/_cluster/state/metadata?multi_project=true");
        ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
        List<Map<String, Object>> projectsMetadata = response.evaluate("metadata.projects");

        Map<String, Map<String, Object>> projectsSettings = new HashMap<>();
        for (Map<String, Object> projectMetadata : projectsMetadata) {
            String id = (String) projectMetadata.get("id");
            @SuppressWarnings("unchecked")
            Map<String, Object> settings = (Map<String, Object>) projectMetadata.get("settings");
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

    @After
    public void removeProjects() throws Exception {
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            deleteProject(project);
            assertProjectObjectStoreClosed(project);
        }
        deleteProject(activeProject);
        assertProjectObjectStoreClosed(activeProject);

        @FixForMultiProject(
            description = "Delete projects via file settings does NOT work currently. This test class does NOT depend on it either."
                + "Uncomment the assertBusy once deletion works."
        )
        final String defaultProjectId = ProjectId.DEFAULT.id();
        // assertBusy(() -> assertProjectIds(adminClient(), List.of(defaultProjectId)));
    }

    private ProjectClient createProjectAndClient(String project) throws IOException {
        return createProjectAndClient(project, false);
    }

    private ProjectClient createProjectAndClient(String project, boolean geoipDownloaderEnabled) throws IOException {
        final int version = RESERVED_STATE_VERSION_COUNTER.incrementAndGet();
        final Path configPath = CONFIG_DIR.getRoot().toPath();
        writeConfigFile(configPath.resolve("operator/project-" + project + ".json"), Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "project_settings": {
                         "ingest.geoip.downloader.enabled": %s,
                         "stateless.object_store.type": "fs",
                         "stateless.object_store.bucket": "project_%s",
                         "stateless.object_store.base_path": "base_path",
                         "stateless.object_store.client": "default"
                     }
                 }
            }""", version, geoipDownloaderEnabled, project));
        writeConfigFile(configPath.resolve("operator/project-" + project + ".secrets.json"), Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {}
            }""", version));

        provisionedProjects.add(project);
        mutableSettings.update(
            Resource.fromString(Strings.format(SETTINGS_JSON_TEMPLATE, projectIdsToCommaSeparatedList(provisionedProjects), version))
        );
        return new ProjectClient(client(), project);
    }

    private void writeConfigFile(Path target, String content) throws IOException {
        final Path directory = target.getParent();
        if (Files.exists(directory) == false) {
            try {
                Files.createDirectories(directory);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        Files.writeString(target, content);
    }

    @FixForMultiProject
    // The API cal to delete the project should not be needed once file-based settings for projects initiates the actual deletion
    private void deleteProject(String project) throws IOException {
        final int version = RESERVED_STATE_VERSION_COUNTER.incrementAndGet();
        provisionedProjects.remove(project);
        mutableSettings.update(
            Resource.fromString(Strings.format(SETTINGS_JSON_TEMPLATE, projectIdsToCommaSeparatedList(provisionedProjects), version))
        );
        final Path configPath = CONFIG_DIR.getRoot().toPath();
        Files.deleteIfExists(configPath.resolve("operator/project-" + project + ".json"));
        Files.deleteIfExists(configPath.resolve("operator/project-" + project + ".secrets.json"));
        // temporarily call the API to delete the project.
        var client = adminClient();
        final Request request = new Request("DELETE", "/_project/" + project);
        try {
            logger.info("--> Deleting project {}", project);
            final Response response = client.performRequest(request);
            logger.info("--> Deleted project {} : {}", project, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("--> Failed to delete project: {}", project, e);
            throw e;
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

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
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
        final ProjectClient projectClient = projectClients.get(activeProject);
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
            createProjectAndClient(newProjectId);
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
            deleteProject(newProjectId);
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
                doTestForOneProjectClient(projectClients.get(projectId));
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
        createProjectAndClient(activeProject, true);
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

    static class ProjectClient {

        private final RestClient delegate;
        private final String projectId;

        ProjectClient(RestClient delegate, String projectId) {
            this.delegate = delegate;
            this.projectId = projectId;
        }

        public String getProjectId() {
            return projectId;
        }

        Response performRequest(Request request) throws IOException {
            setRequestProjectId(request);
            return delegate.performRequest(request);
        }

        void setRequestProjectId(Request request) {
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
            options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
            request.setOptions(options);
        }
    }
}
