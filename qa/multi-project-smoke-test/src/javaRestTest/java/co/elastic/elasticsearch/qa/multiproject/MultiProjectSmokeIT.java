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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

public class MultiProjectSmokeIT extends ESRestTestCase {

    private static final String ADMIN_USERNAME = "admin-user";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static String activeProject = null;
    private static Set<String> extraProjects = null;

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("stateless.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .user(ADMIN_USERNAME, ADMIN_PASSWORD)
        .setting("xpack.watcher.enabled", "false")
        .setting("multi_project.enabled", "true")
        .build();

    @BeforeClass
    public static void randomizeProjectIds() {
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 5, ESTestCase::randomIdentifier);
    }

    private Map<String, ProjectClient> projectClients;

    @Before
    public void configureProjects() throws Exception {
        initClient();
        projectClients = new HashMap<>();
        projectClients.put(activeProject, createProject(activeProject));
        for (var project : extraProjects) {
            projectClients.put(project, createProject(project));
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
    public void removeProjects() throws Exception {
        for (var project : extraProjects) {
            deleteProject(project);
        }
        deleteProject(activeProject);
    }

    private ProjectClient createProject(String project) throws IOException {
        RestClient client = adminClient();
        final Request request = new Request("PUT", "/_project/" + project);
        try {
            logger.info("--> Creating project {}", project);
            final Response response = client.performRequest(request);
            logger.info("--> Created project {} : {}", project, response.getStatusLine());
            return new ProjectClient(client(), project);
        } catch (ResponseException e) {
            logger.error("--> Failed to create project: {}", project);
            throw e;
        }
    }

    private void deleteProject(String project) throws IOException {
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

    private void assertProjectIds(RestClient client, List<String> expectedProjects) throws IOException {
        final Collection<String> actualProjects = getProjectIds(client);
        assertThat(
            "Cluster returned project ids: " + actualProjects,
            actualProjects,
            containsInAnyOrder(expectedProjects.toArray(String[]::new))
        );
    }

    protected Collection<String> getProjectIds(RestClient client) throws IOException {
        final Request request = new Request("GET", "/_cluster/state/routing_table?multi_project=true");
        try {
            final ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
            final List<Map<String, Object>> projectRouting = response.evaluate("routing_table.projects");
            return projectRouting.stream().map(obj -> (String) obj.get("id")).toList();
        } catch (ResponseException e) {
            logger.error("--> Failed to retrieve cluster state", e);
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
                projectClient.performRequest(new Request("GET", "/_search?size=" + numDocs))
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
