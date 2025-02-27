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

package co.elastic.elasticsearch.serverless.multiproject;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class MultiProjectFileSettingsServiceIT extends ESIntegTestCase {

    private final AtomicLong versionCounter = new AtomicLong(1);

    @Before
    public void resetVersionCounter() {
        versionCounter.set(1);
    }

    @FixForMultiProject // use some actual state, when we have project-specific state handlers
    private static final String emptyProjectSettings = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {}
        }""";

    @FixForMultiProject // use some actual state, when we have project-specific state handlers
    private static final String emptyProjectSecrets = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {}
        }""";

    private static final String testJSON = """
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

    private static final String testJSON43mb = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "43mb"
                 }
             }
        }""";

    private static final String testCleanupJSON = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {}
             }
        }""";

    private static final String testErrorJSON = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "not_cluster_settings": {
                     "search.allow_expensive_queries": "false"
                 }
             }
        }""";

    private static final String testOtherErrorJSON = """
        {
             "projects": %s,
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "bad_cluster_settings": {
                     "search.allow_expensive_queries": "false"
                 }
             }
        }""";

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(super.buildEnvSettings(settings))
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
        plugins.add(ServerlessMultiProjectPlugin.class);
        return plugins;
    }

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode().getName(),
            equalTo(node)
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
        writeFileWithRetry(node, jsonWithVersion, fileSettingsService.watchedFile(), logger);
    }

    private static void writeProjectFile(String node, String json, String project, Logger logger, Long version) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        writeFileWithRetry(
            node,
            Strings.format(json, version),
            fileSettingsService.watchedFileDir().resolve("project-" + project + ".json"),
            logger
        );
    }

    private static void writeSecretsFile(String node, String json, String project, Logger logger, Long version) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());
        writeFileWithRetry(
            node,
            Strings.format(json, version),
            fileSettingsService.watchedFileDir().resolve("project-" + project + ".secrets.json"),
            logger
        );
    }

    private static void writeFileWithRetry(String node, String jsonWithVersion, Path targetPath, Logger logger) throws IOException,
        InterruptedException {
        Path tempFilePath = createTempFile();
        logger.info("--> before writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(jsonWithVersion);

        Files.writeString(tempFilePath, jsonWithVersion);
        int retryCount = 0;
        do {
            try {
                // this can fail on Windows because of timing
                Files.move(tempFilePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
                logger.info("--> after writing JSON config to node {} with path {}", node, tempFilePath);
                return;
            } catch (IOException e) {
                logger.info("--> retrying writing a settings file [{}]", retryCount);
                if (retryCount == 4) { // retry 5 times
                    throw e;
                }
                Thread.sleep(retryDelay(retryCount));
                retryCount++;
            }
        } while (true);
    }

    private static long retryDelay(int retryCount) {
        return 100 * (1 << retryCount) + Randomness.get().nextInt(10);
    }

    private Tuple<CountDownLatch, AtomicLong> setupCleanupClusterStateListener(List<String> nodes) {
        return setupClusterStateListener(nodes, Map.of(), Set.of());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(
        List<String> nodes,
        Map<ProjectId, Long> expectedVersionById,
        Set<String> expectedHandlerKeys
    ) {
        List<ClusterService> clusterServices = nodes.stream().map(node -> internalCluster().clusterService(node)).toList();
        CountDownLatch savedClusterState = new CountDownLatch(nodes.size());
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterServices.forEach(clusterService -> clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                Metadata metadata = event.state().metadata();
                if (reservedClusterStateHasHandlerKeys(
                    metadata.reservedStateMetadata().get(FileSettingsService.NAMESPACE),
                    expectedHandlerKeys
                ) && allProjectsOnExpectedVersion(metadata.projects(), expectedVersionById)) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        }));

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private boolean allProjectsOnExpectedVersion(Map<ProjectId, ProjectMetadata> projectStates, Map<ProjectId, Long> expectedVersionsById) {
        if (projectStates.keySet().containsAll(expectedVersionsById.keySet()) == false) {
            return false;
        }
        return expectedVersionsById.entrySet()
            .stream()
            .allMatch(
                entry -> projectStates.get(entry.getKey())
                    .reservedStateMetadata()
                    .get(MultiProjectFileSettingsService.SECRETS_NAMESPACE)
                    .version()
                    .equals(entry.getValue())
            );
    }

    private boolean reservedClusterStateHasHandlerKeys(ReservedStateMetadata reservedState, Set<String> expectedHandlerKeys) {
        if (reservedState != null) {
            ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
            return handlerMetadata != null && handlerMetadata.keys().equals(expectedHandlerKeys);
        }
        return expectedHandlerKeys.isEmpty();
    }

    private void assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion, String expectedBytesPerSec)
        throws Exception {
        assertTrue(savedClusterState.await(20, TimeUnit.SECONDS));
        assertExpectedRecoveryBytesSettingAndVersion(metadataVersion, expectedBytesPerSec);
    }

    private static void assertExpectedRecoveryBytesSettingAndVersion(AtomicLong metadataVersion, String expectedBytesPerSec) {
        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        assertThat(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo(expectedBytesPerSec)
        );

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        assertThat(
            expectThrows(ExecutionException.class, () -> clusterAdmin().updateSettings(req).get()).getMessage(),
            is(
                "java.lang.IllegalArgumentException: Failed to process request "
                    + "[org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest/unset] "
                    + "with errors: [[indices.recovery.max_bytes_per_sec] set as read-only by [file_settings]]"
            )
        );
    }

    public void testSettingsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        ProjectId projectId = randomUniqueProjectId();

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(wrap(internalCluster().nonMasterClient()), masterNode);
        var savedClusterState = setupClusterStateListener(
            List.of(masterNode, dataNode),
            Map.of(projectId, 4L),
            Set.of(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));
        assertFalse(dataFileSettingsService.watching());

        writeSettingsFile(masterNode, testJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(masterNode, emptyProjectSettings, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(masterNode, emptyProjectSecrets, projectId.id(), logger, versionCounter.incrementAndGet());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");
    }

    public void testSettingsAppliedOnStart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        ProjectId projectId = randomUniqueProjectId();

        assertFalse(dataFileSettingsService.watching());

        var savedClusterState = setupClusterStateListener(
            List.of(dataNode),
            Map.of(projectId, 4L),
            Set.of(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // In internal cluster tests, the nodes share the config directory, so when we write with the data node path
        // the master will pick it up on start
        writeSettingsFile(dataNode, testJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(dataNode, emptyProjectSettings, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(dataNode, emptyProjectSecrets, projectId.id(), logger, versionCounter.incrementAndGet());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(wrap(internalCluster().nonMasterClient()), masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));
        assertFalse(dataFileSettingsService.watching());

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");
    }

    public void testReservedStatePersistsOnRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(wrap(internalCluster().masterClient()), masterNode);

        ProjectId projectId = randomUniqueProjectId();
        var savedClusterState = setupClusterStateListener(
            List.of(masterNode),
            Map.of(projectId, 4L),
            Set.of(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));

        logger.info("--> write some settings");
        writeSettingsFile(masterNode, testJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(masterNode, emptyProjectSettings, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(masterNode, emptyProjectSecrets, projectId.id(), logger, versionCounter.incrementAndGet());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");

        logger.info("--> restart master");
        internalCluster().restartNode(masterNode);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
        assertThat(
            clusterStateResponse.getState()
                .metadata()
                .reservedStateMetadata()
                .get(FileSettingsService.NAMESPACE)
                .handlers()
                .get(ReservedClusterSettingsAction.NAME)
                .keys(),
            hasSize(1)
        );
        assertThat(clusterStateResponse.getState().metadata().projects(), hasKey(projectId));
    }

    private Client wrap(Client client) {
        final SecurityContext context = new SecurityContext(client.settings(), client.threadPool().getThreadContext());
        return new FilterClient(client) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                context.executeAsSystemUser(
                    original -> super.doExecute(
                        action,
                        request,
                        new ContextPreservingActionListener<>(context.getThreadContext().wrapRestorable(original), listener)
                    )
                );
            }
        };
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("Missing handler definition for content key [not_cluster_settings]")
                    );
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        assertThat(clusterStateResponse.getState().metadata().persistentSettings().get("search.allow_expensive_queries"), nullValue());

        // This should succeed, nothing was reserved
        updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", "false"));
    }

    public void testErrorSaved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));
        assertFalse(dataFileSettingsService.watching());

        writeSettingsFile(masterNode, testErrorJSON, List.of(), logger, versionCounter.incrementAndGet());
        assertClusterStateNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    public void testErrorCanRecoverOnRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(wrap(internalCluster().nonMasterClient()), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));
        assertFalse(dataFileSettingsService.watching());

        writeSettingsFile(masterNode, testErrorJSON, List.of(), logger, versionCounter.incrementAndGet());
        AtomicLong metadataVersion = savedClusterState.v2();
        assertClusterStateNotSaved(savedClusterState.v1(), metadataVersion);
        assertHasErrors(metadataVersion, "not_cluster_settings");

        // write valid json without version increment to simulate ES being able to process settings after a restart (usually, this would be
        // due to a code change)
        writeSettingsFile(masterNode, testJSON, List.of(), logger, versionCounter.get());
        internalCluster().restartNode(masterNode);
        ensureGreen();

        // we don't know the exact metadata version to wait for so rely on an assertBusy instead
        assertBusy(() -> assertExpectedRecoveryBytesSettingAndVersion(metadataVersion, "50mb"));
        assertBusy(() -> assertNoErrors(metadataVersion));
    }

    public void testNewErrorOnRestartReprocessing() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(wrap(internalCluster().nonMasterClient()), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(waitUntil(masterFileSettingsService::watching));
        assertFalse(dataFileSettingsService.watching());

        writeSettingsFile(masterNode, testErrorJSON, List.of(), logger, versionCounter.incrementAndGet());
        AtomicLong metadataVersion = savedClusterState.v2();
        assertClusterStateNotSaved(savedClusterState.v1(), metadataVersion);
        assertHasErrors(metadataVersion, "not_cluster_settings");

        // write json with new error without version increment to simulate ES failing to process settings after a restart for a new reason
        // (usually, this would be due to a code change)
        writeSettingsFile(masterNode, testOtherErrorJSON, List.of(), logger, versionCounter.get());
        assertHasErrors(metadataVersion, "not_cluster_settings");
        internalCluster().restartNode(masterNode);
        ensureGreen();

        assertBusy(() -> assertHasErrors(metadataVersion, "bad_cluster_settings"));
    }

    public void testSettingsAppliedOnMasterReElection() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();

        logger.info("--> start master eligible nodes, 2 more for quorum");
        String masterNode1 = internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        String masterNode2 = internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        internalCluster().validateClusterFormed();
        ensureStableCluster(3);

        FileSettingsService master1FS = internalCluster().getInstance(FileSettingsService.class, masterNode1);
        FileSettingsService master2FS = internalCluster().getInstance(FileSettingsService.class, masterNode2);

        assertFalse(master1FS.watching());
        assertFalse(master2FS.watching());

        ProjectId projectId = randomUniqueProjectId();

        // Make sure cluster state is applied on all nodes
        var savedClusterState = setupClusterStateListener(
            List.of(masterNode, masterNode1, masterNode2),
            Map.of(projectId, 4L),
            Set.of("indices.recovery.max_bytes_per_sec")
        );
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());

        writeSettingsFile(masterNode, testJSON, List.of(projectId.id()), logger, versionCounter.incrementAndGet());
        writeProjectFile(masterNode, emptyProjectSettings, projectId.id(), logger, versionCounter.incrementAndGet());
        writeSecretsFile(masterNode, emptyProjectSecrets, projectId.id(), logger, versionCounter.incrementAndGet());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");

        internalCluster().stopCurrentMasterNode();
        internalCluster().validateClusterFormed();
        ensureStableCluster(2);

        FileSettingsService masterFS = internalCluster().getCurrentMasterNodeInstance(FileSettingsService.class);
        assertTrue(masterFS.watching());
        logger.info("--> start another master eligible node to form a quorum");
        String masterNode3 = internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        internalCluster().validateClusterFormed();
        ensureStableCluster(3);

        savedClusterState = setupCleanupClusterStateListener(
            List.of(internalCluster().getMasterName(), masterNode3, masterNode1, masterNode2)
        );
        writeSettingsFile(internalCluster().getMasterName(), testCleanupJSON, List.of(), logger, versionCounter.incrementAndGet());

        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        savedClusterState = setupClusterStateListener(
            List.of(masterNode1, masterNode2, masterNode3),
            Map.of(projectId, 4L),
            Set.of("indices.recovery.max_bytes_per_sec")
        );
        writeSettingsFile(internalCluster().getMasterName(), testJSON43mb, List.of(), logger, versionCounter.incrementAndGet());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "43mb");
    }

    private void assertHasErrors(AtomicLong waitForMetadataVersion, String expectedError) {
        var errorMetadata = getErrorMetadata(waitForMetadataVersion);
        assertThat(errorMetadata, is(notNullValue()));
        assertThat(errorMetadata.errors(), containsInAnyOrder(containsString(expectedError)));
    }

    private void assertNoErrors(AtomicLong waitForMetadataVersion) {
        var errorMetadata = getErrorMetadata(waitForMetadataVersion);
        assertThat(errorMetadata, is(nullValue()));
    }

    private ReservedStateErrorMetadata getErrorMetadata(AtomicLong waitForMetadataVersion) {
        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(waitForMetadataVersion.get())
        ).actionGet();
        return clusterStateResponse.getState().getMetadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE).errorMetadata();
    }
}
