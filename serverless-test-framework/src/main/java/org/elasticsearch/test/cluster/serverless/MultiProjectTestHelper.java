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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.junit.Assert.assertTrue;

public class MultiProjectTestHelper {

    private static final Logger logger = LogManager.getLogger(MultiProjectTestHelper.class);

    private final TemporaryFolder configDir;
    private final AtomicLong reservedStateVersionCounter;
    private final Set<String> provisionedProjects;

    public MultiProjectTestHelper(TemporaryFolder configDir, AtomicLong reservedStateVersionCounter) {
        this.configDir = configDir;
        this.provisionedProjects = ConcurrentCollections.newConcurrentSet();
        this.reservedStateVersionCounter = reservedStateVersionCounter;
    }

    public void putProject(RestClient adminClient, String projectId) throws Exception {
        putProject(adminClient, projectId, Settings.EMPTY, Settings.EMPTY);
    }

    public void putProject(RestClient adminClient, String projectId, Settings projectSettings) throws Exception {
        putProject(adminClient, projectId, projectSettings, Settings.EMPTY);
    }

    public void putProject(RestClient adminClient, String projectId, Settings projectSettings, Settings projectSecrets) throws Exception {
        final var added = provisionedProjects.add(projectId);
        logger.info("--> {} project [{}]", added ? "creating" : "updating", projectId);
        final long version = reservedStateVersionCounter.incrementAndGet();
        final Path configPath = configDir.getRoot().toPath();
        writeProjectSettingsFile(
            projectId,
            configPath,
            version,
            Settings.builder().put(baseProjectSettings(projectId)).put(projectSettings).build()
        );
        writeProjectSecretsFile(projectId, configPath, version, projectSecrets);
        writeSettingsFile(configPath, version);
        assertProjectAdded(adminClient, projectId);
    }

    public void removeProject(RestClient adminClient, String projectId) throws Exception {
        final boolean removed = provisionedProjects.remove(projectId);
        if (removed == false) {
            logger.info("--> project [{}] does not exist in the test cluster", projectId);
            return;
        }
        logger.info("--> deleting project [{}]", projectId);
        final long version = reservedStateVersionCounter.incrementAndGet();
        final Path configPath = configDir.getRoot().toPath();
        writeSettingsFile(configPath, version);
        Files.deleteIfExists(configPath.resolve("operator/project-" + projectId + ".json"));
        Files.deleteIfExists(configPath.resolve("operator/project-" + projectId + ".secrets.json"));
        @FixForMultiProject(
            description = "Delete projects via file settings does NOT work currently. Temporarily call the API to delete the project."
        )
        final Request request = new Request("DELETE", "/_project/" + projectId);
        try {
            adminClient.performRequest(request);
        } catch (ResponseException e) {
            logger.error("--> failed to delete project: {}", projectId, e);
            throw e;
        }
    }

    public static ProjectClient projectClient(RestClient restClient, String projectId) {
        return new ProjectClient(restClient, projectId);
    }

    private Settings baseProjectSettings(String projectId) {
        return Settings.builder()
            .put("stateless.object_store.type", "fs")
            .put("stateless.object_store.bucket", "project_" + projectId)
            .put("stateless.object_store.base_path", "base_path")
            .put("stateless.object_store.client", "default")
            .build();
    }

    private void writeProjectSettingsFile(String projectId, Path configPath, long version, Settings projectSettings) throws IOException {
        writeConfigFile(configPath.resolve("operator/project-" + projectId + ".json"), Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "project_settings": %s
                 }
            }""", version, projectSettings.toString()));
    }

    private void writeProjectSecretsFile(String projectId, Path configPath, long version, Settings stringSecrets) throws IOException {
        writeProjectSecretsFile(projectId, configPath, version, stringSecrets, Settings.EMPTY);
    }

    private void writeProjectSecretsFile(String projectId, Path configPath, long version, Settings stringSecrets, Settings fileSecrets)
        throws IOException {
        writeConfigFile(configPath.resolve("operator/project-" + projectId + ".secrets.json"), Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "project_secrets": {
                        "string_secrets": %s,
                        "file_secrets": %s
                     }
                 }
            }""", version, stringSecrets.toString(), fileSecrets.toString()));
    }

    private void writeSettingsFile(Path configPath, long version) throws IOException {
        writeConfigFile(configPath.resolve("operator/settings.json"), Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                 },
                 "projects": [%s]
            }""", version, projectIdsToCommaSeparatedList(provisionedProjects)));
    }

    private static String projectIdsToCommaSeparatedList(Set<String> projectIds) {
        return projectIds.stream().map(id -> '"' + id + '"').collect(Collectors.joining(","));
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

    private void assertProjectAdded(RestClient adminClient, String projectId) throws Exception {
        assertBusy(() -> {
            final Request request = new Request("GET", "/_cluster/state/metadata?multi_project=true");
            final ObjectPath response = ObjectPath.createFromResponse(adminClient.performRequest(request));
            final List<Map<String, Object>> projectMetadatas = response.evaluate("metadata.projects");
            final List<String> projectIds = projectMetadatas.stream().map(obj -> (String) obj.get("id")).toList();
            assertTrue("project [" + projectId + "] not found in " + projectIds, projectIds.contains(projectId));
        });
    }

    public static class ProjectClient {

        private final RestClient delegate;
        private final String projectId;

        ProjectClient(RestClient delegate, String projectId) {
            this.delegate = delegate;
            this.projectId = projectId;
        }

        public String getProjectId() {
            return projectId;
        }

        public Response performRequest(Request request) throws IOException {
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
