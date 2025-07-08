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

package co.elastic.elasticsearch.settings.secure;

import co.elastic.elasticsearch.serverless.multiproject.MultiProjectFileSettingsService;
import co.elastic.elasticsearch.serverless.multiproject.ServerlessMultiProjectPlugin;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class MultiProjectFileSecureSettingsIT extends ESIntegTestCase {
    private static final AtomicLong versionCounter = new AtomicLong(1);

    private static final String testProjectJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {}
        }""";

    private static final String testSettingsJSON = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "50mb"
                 }
             }
        }""";

    private static final String testSecretsJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "project_secrets": {
                    "string_secrets": {
                        "test.setting": "%s"
                     },
                    "file_secrets": {
                        "test.file.setting": "%s"
                    }
                 }
             }
        }""";

    private static final String testErrorJSON = """
        {
            "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
            },
            "state": {
                 "not_project_secrets": {
                    "string_secrets": {
                        "not_written.hopefully": "does_not_matter"
                    }
                 }
             }
        }""";

    @Override
    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(super.buildEnvSettings(settings))
            .put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ServerlessSecureSettingsPlugin.class);
        plugins.add(ServerlessMultiProjectPlugin.class);
        return plugins;
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    public void testSecureSettingsAppliedOnRunningNode() throws Exception {
        // Setup nodes
        internalCluster().setBootstrapMasterNodeIndex(0);
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        final String masterNode = internalCluster().startMasterOnlyNode();
        awaitMasterNode(dataNode, masterNode);
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        // Make sure the secure settings dir is only watched on the master
        FileSettingsService masterMultiProjectFileSecureSettingsService = internalCluster().getInstance(
            FileSettingsService.class,
            masterNode
        );
        assertTrue(waitUntil(masterMultiProjectFileSecureSettingsService::watching));

        FileSettingsService dataMultiProjectFileSecureSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataMultiProjectFileSecureSettingsService.watching());

        // Test data
        ProjectId projectId = randomUniqueProjectId();
        String testStringSettingsValue = randomAlphaOfLengthBetween(0, 100);
        byte[] testFileSettingsValue = randomByteArrayOfLength(randomIntBetween(0, 100));
        String testFileSettingsValueEncoded = Base64.getEncoder().encodeToString(testFileSettingsValue);

        // Setup cluster state listeners to wait for project data to be available
        var createdProjectsLatch = setupProjectsCreatedLatch(masterNode, Set.of(projectId));
        var projectsWithSecretsLatch = setupProjectsWithSecretsLatch(masterNode, Set.of(projectId));

        // Create project
        writeSettingsFile(masterNode, testSettingsJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(masterNode, testProjectJSON, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(
            masterNode,
            Strings.format(testSecretsJSON, versionCounter.get(), testStringSettingsValue, testFileSettingsValueEncoded),
            projectId.id(),
            logger
        );
        safeAwait(createdProjectsLatch.v1());
        safeAwait(projectsWithSecretsLatch.v1());

        assertProjectSecrets(projectsWithSecretsLatch.v2().get(projectId), testStringSettingsValue, testFileSettingsValue);
    }

    public void testSecureSettingsAppliedOnStart() throws Exception {
        // Setup data node
        internalCluster().setBootstrapMasterNodeIndex(0);
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        FileSettingsService dataMultiProjectFileSecureSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataMultiProjectFileSecureSettingsService.watching());

        // Test data
        ProjectId projectId = randomUniqueProjectId();
        String testStringSettingsValue = randomAlphaOfLengthBetween(0, 100);
        byte[] testFileSettingsValue = randomByteArrayOfLength(randomIntBetween(0, 100));
        String testFileSettingsValueEncoded = Base64.getEncoder().encodeToString(testFileSettingsValue);

        // Setup cluster state listeners to wait for project data being available (data node should get cluster state updates)
        var createdProjectsLatch = setupProjectsCreatedLatch(dataNode, Set.of(projectId));
        var projectsWithSecretsLatch = setupProjectsWithSecretsLatch(dataNode, Set.of(projectId));

        writeSettingsFile(dataNode, testSettingsJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(dataNode, testProjectJSON, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(
            dataNode,
            Strings.format(testSecretsJSON, versionCounter.get(), testStringSettingsValue, testFileSettingsValueEncoded),
            projectId.id(),
            logger
        );

        // Start master node
        final String masterNode = internalCluster().startMasterOnlyNode();
        awaitMasterNode(dataNode, masterNode);
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        safeAwait(createdProjectsLatch.v1());
        safeAwait(projectsWithSecretsLatch.v1());

        assertProjectSecrets(projectsWithSecretsLatch.v2().get(projectId), testStringSettingsValue, testFileSettingsValue);
    }

    public void testReservedStateSecretKeysReAppliedOnRestart() throws Exception {
        // Setup node
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().masterClient(), masterNode);

        FileSettingsService masterMultiProjectFileSecureSettingsService = internalCluster().getInstance(
            FileSettingsService.class,
            masterNode
        );

        assertTrue(waitUntil(masterMultiProjectFileSecureSettingsService::watching));

        // Test data
        ProjectId projectId = randomUniqueProjectId();
        String testStringSettingsValue = randomAlphaOfLengthBetween(0, 100);
        byte[] testFileSettingsValue = randomByteArrayOfLength(randomIntBetween(0, 100));
        String testFileSettingsValueEncoded = Base64.getEncoder().encodeToString(testFileSettingsValue);

        // Setup cluster state listeners to wait for project data being available (data node should get cluster state updates)
        {
            var createdProjectsLatch = setupProjectsCreatedLatch(masterNode, Set.of(projectId));
            var projectsWithSecretsLatch = setupProjectsWithSecretsLatch(masterNode, Set.of(projectId));

            // Write settings, project and secrets file
            writeSettingsFile(masterNode, testSettingsJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
            writeSecretsFile(
                masterNode,
                Strings.format(testSecretsJSON, versionCounter.incrementAndGet(), testStringSettingsValue, testFileSettingsValueEncoded),
                projectId.id(),
                logger
            );
            writeProjectFile(masterNode, testProjectJSON, projectId.id(), logger, versionCounter.get());
            safeAwait(createdProjectsLatch.v1());
            safeAwait(projectsWithSecretsLatch.v1());
            assertProjectSecrets(projectsWithSecretsLatch.v2().get(projectId), testStringSettingsValue, testFileSettingsValue);
        }
        internalCluster().restartNode(masterNode);
        // Setup cluster state listeners to wait for project secrets to be available again after restart
        {
            var projectsWithSecretsLatch = setupProjectsWithSecretsLatch(masterNode, Set.of(projectId));
            safeAwait(projectsWithSecretsLatch.v1());
            assertProjectSecrets(projectsWithSecretsLatch.v2().get(projectId), testStringSettingsValue, testFileSettingsValue);
        }
    }

    public void testErrorSaved() throws Exception {
        // Setup node
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().masterClient(), masterNode);

        FileSettingsService masterMultiProjectFileSecureSettingsService = internalCluster().getInstance(
            FileSettingsService.class,
            masterNode
        );

        assertTrue(waitUntil(masterMultiProjectFileSecureSettingsService::watching));

        // Test data
        ProjectId projectId = randomUniqueProjectId();
        String testStringSettingsValue = randomAlphaOfLengthBetween(0, 100);
        byte[] testFileSettingsValue = randomByteArrayOfLength(randomIntBetween(0, 100));
        String testFileSettingsValueEncoded = Base64.getEncoder().encodeToString(testFileSettingsValue);

        // Setup cluster state listeners to wait for project data being available (data node should get cluster state updates)
        var createdProjectsLatch = setupProjectsCreatedLatch(masterNode, Set.of(projectId));
        var projectsWithSecretsLatch = setupProjectsWithSecretsLatch(masterNode, Set.of(projectId));

        // Write settings, project and secrets file
        writeSettingsFile(masterNode, testSettingsJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeSecretsFile(
            masterNode,
            Strings.format(testSecretsJSON, versionCounter.incrementAndGet(), testStringSettingsValue, testFileSettingsValueEncoded),
            projectId.id(),
            logger
        );
        writeProjectFile(masterNode, testProjectJSON, projectId.id(), logger, versionCounter.get());

        safeAwait(createdProjectsLatch.v1());
        safeAwait(projectsWithSecretsLatch.v1());

        assertProjectSecrets(projectsWithSecretsLatch.v2().get(projectId), testStringSettingsValue, testFileSettingsValue);

        var errorOccurredLatch = setupErrorOccurredLatch(
            masterNode,
            projectId,
            "Missing handler definition for content key [not_project_secrets]"
        );

        writeProjectFile(masterNode, testProjectJSON, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(masterNode, Strings.format(testErrorJSON, versionCounter.get()), projectId.id(), logger);
        assertReservedMetadataNotSaved(errorOccurredLatch, projectId, "not_written.hopefully");
    }

    private void assertProjectSecrets(ProjectSecrets projectSecrets, String stringSetting, byte[] fileSetting)
        throws GeneralSecurityException, IOException {
        assertNotNull(projectSecrets);
        assertEquals(stringSetting, projectSecrets.getSettings().getString("test.setting").toString());
        assertArrayEquals(fileSetting, projectSecrets.getSettings().getFile("test.file.setting").readAllBytes());
    }

    private void assertReservedMetadataNotSaved(CountDownLatch errorOccurredLatch, ProjectId projectId, String keyNotSaved) {
        safeAwait(errorOccurredLatch);
        ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        var metadata = clusterService.state().projectState(projectId).metadata();
        ProjectSecrets projectSecrets = metadata.custom(ProjectSecrets.TYPE);
        assertThat(projectSecrets.getSettings().getString(keyNotSaved), nullValue());
    }

    private CountDownLatch setupErrorOccurredLatch(String node, ProjectId projectId, String expectedError) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch errorOccurredLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().metadata().hasProject(projectId) == false) {
                    return;
                }

                ReservedStateMetadata reservedStateMetadata = event.state()
                    .metadata()
                    .getProject(projectId)
                    .reservedStateMetadata()
                    .get(MultiProjectFileSettingsService.NAMESPACE);
                if (reservedStateMetadata != null
                    && reservedStateMetadata.errorMetadata() != null
                    && reservedStateMetadata.errorMetadata().errors().get(0).contains(expectedError)) {
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedStateMetadata.errorMetadata().errorKind());
                    assertThat(reservedStateMetadata.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    clusterService.removeListener(this);
                    errorOccurredLatch.countDown();
                }
            }
        });

        return errorOccurredLatch;
    }

    private Tuple<CountDownLatch, Map<ProjectId, ProjectMetadata>> setupProjectsCreatedLatch(String node, Set<ProjectId> expectedProjects) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        Map<ProjectId, ProjectMetadata> projectMetadataByProject = new HashMap<>();
        clusterService.addListener(event -> {
            event.state().forEachProject(projectState -> projectMetadataByProject.put(projectState.projectId(), projectState.metadata()));
            if (projectMetadataByProject.keySet().containsAll(expectedProjects)) {
                savedClusterState.countDown();
            }
        });

        return new Tuple<>(savedClusterState, projectMetadataByProject);
    }

    private void countDownLatchIfAllProjectsCreated(
        ClusterState state,
        CountDownLatch latch,
        Map<ProjectId, ProjectSecrets> secretsById,
        Set<ProjectId> expectedProjects
    ) {
        Map<ProjectId, ProjectMetadata> projectMetadataByProject = new HashMap<>();

        state.forEachProject(projectState -> projectMetadataByProject.put(projectState.projectId(), projectState.metadata()));
        if (projectMetadataByProject.keySet().containsAll(expectedProjects)) {
            projectMetadataByProject.forEach((key, value) -> secretsById.put(key, value.custom(ProjectSecrets.TYPE, null)));

            if (expectedProjects.stream().allMatch(projectId -> secretsById.get(projectId) != null)) {
                latch.countDown();
            }
        }
    }

    private Tuple<CountDownLatch, Map<ProjectId, ProjectSecrets>> setupProjectsWithSecretsLatch(
        String node,
        Set<ProjectId> expectedProjects
    ) {
        ClusterService clusterService = internalCluster().clusterService(node);
        Map<ProjectId, ProjectSecrets> secretsById = new HashMap<>();
        CountDownLatch savedClusterState = new CountDownLatch(1);

        clusterService.addListener(
            event -> countDownLatchIfAllProjectsCreated(event.state(), savedClusterState, secretsById, expectedProjects)
        );
        countDownLatchIfAllProjectsCreated(clusterService.state(), savedClusterState, secretsById, expectedProjects);

        return new Tuple<>(savedClusterState, secretsById);
    }

    private static void writeSecretsFile(String node, String json, String projectId, Logger logger) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());

        writeFileWithRetry(
            json,
            fileSettingsService.watchedFileDir().resolve(Strings.format("project-%s.secrets.json", projectId)),
            logger
        );
    }

    private static void writeSettingsFile(String node, String json, Collection<String> projects, Logger logger, Long version)
        throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());

        String jsonWithVersion = Strings.format(
            json,
            projects.isEmpty() ? "[]" : projects.stream().collect(Collectors.joining("\",\"", "[\"", "\"]")),
            version
        );

        writeFileWithRetry(jsonWithVersion, fileSettingsService.watchedFile(), logger);
    }

    private static void writeProjectFile(String node, String json, String project, Logger logger, Long version) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());
        String jsonWithVersion = Strings.format(json, version);

        writeFileWithRetry(jsonWithVersion, fileSettingsService.watchedFileDir().resolve("project-" + project + ".json"), logger);
    }

    private static void writeFileWithRetry(String json, Path target, Logger logger) throws Exception {
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, json);
        final AtomicInteger retryCount = new AtomicInteger(0);
        logger.info("Writing JSON config with path [{}]", tempFilePath);
        assertBusy(() -> {
            try {
                // this can fail on Windows because of timing
                Files.move(tempFilePath, target, StandardCopyOption.ATOMIC_MOVE);
                logger.info("Successfully JSON config with path [{}]", tempFilePath);
            } catch (IOException e) {
                retryCount.incrementAndGet();
                logger.info("Retrying writing file [{}], retry number=[{}]", target, retryCount.get());
                fail();
            }
        }, 20, TimeUnit.SECONDS);
    }

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).execute().actionGet().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }
}
