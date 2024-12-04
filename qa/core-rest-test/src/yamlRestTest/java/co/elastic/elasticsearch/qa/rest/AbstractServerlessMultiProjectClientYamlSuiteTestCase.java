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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractServerlessMultiProjectClientYamlSuiteTestCase extends ESClientYamlSuiteTestCase {
    public static final boolean MULTI_PROJECT_ENABLED = Boolean.parseBoolean(System.getProperty("es.test.multi_project.enabled", "false"));
    // The active project-id is slightly longer, and has a fixed suffix so that it's easier to pick in error messages etc.
    private final String activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
    private final Set<String> extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));

    public AbstractServerlessMultiProjectClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    public void configureProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
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
    public void removeProjects() throws Exception {
        if (MULTI_PROJECT_ENABLED == false) {
            return;
        }
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            assertEmptyProject(project);
            deleteProject(project);
        }
        deleteProject(activeProject);
    }

    private void createProject(String project) throws IOException {
        RestClient client = adminClient();
        final Request request = new Request("PUT", "/_project/" + project);
        try {
            logger.info("--> Creating project {}", project);
            final Response response = client.performRequest(request);
            logger.info("--> Created project {} : {}", project, response.getStatusLine());
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

    private void assertEmptyProject(String projectId) throws IOException {
        logger.info("--> asserting empty project {}", projectId);
        final Request request = new Request("GET", "_cluster/state/metadata,routing_table,customs");
        request.setOptions(request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId).build());

        var response = responseAsMap(adminClient().performRequest(request));
        ObjectPath state = new ObjectPath(response);

        assertThat(
            "Project [" + projectId + "] should not have indices",
            ((Map<?, ?>) state.evaluate("metadata.indices")).keySet(),
            empty()
        );
        assertThat(
            "Project [" + projectId + "] should not have routing entries",
            ((Map<?, ?>) state.evaluate("routing_table.indices")).keySet(),
            empty()
        );
        assertThat(
            "Project [" + projectId + "] should not have graveyard entries",
            state.evaluate("metadata.index-graveyard.tombstones"),
            empty()
        );

        final Map<String, ?> legacyTemplates = state.evaluate("metadata.templates");
        if (legacyTemplates != null) {
            var templateNames = legacyTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have legacy templates", templateNames, empty());
        }

        final Map<String, Object> indexTemplates = state.evaluate("metadata.index_template.index_template");
        if (indexTemplates != null) {
            var templateNames = indexTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have index templates", templateNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard templates, but was null");
        }

        final Map<String, Object> componentTemplates = state.evaluate("metadata.component_template.component_template");
        if (componentTemplates != null) {
            var templateNames = componentTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have component templates", templateNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard component templates, but was null");
        }

        final List<Map<String, ?>> pipelines = state.evaluate("metadata.ingest.pipeline");
        if (pipelines != null) {
            var pipelineNames = pipelines.stream()
                .map(pipeline -> String.valueOf(pipeline.get("id")))
                .filter(id -> isXPackIngestPipeline(id) == false)
                .toList();
            assertThat("Project [" + projectId + "] should not have ingest pipelines", pipelineNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard ingest pipelines, but was null");
        }

        final Map<String, Object> ilmPolicies = state.evaluate("metadata.index_lifecycle.policies");
        assertThat("Expected no ILM policies in serverless", ilmPolicies, nullValue());
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
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (MULTI_PROJECT_ENABLED && projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, activeProject);
        }
        return builder.build();
    }
}
