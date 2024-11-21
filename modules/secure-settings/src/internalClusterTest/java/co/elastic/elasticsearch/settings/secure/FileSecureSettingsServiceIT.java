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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class FileSecureSettingsServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessSecureSettingsPlugin.class, TestSecureSettingPlugin.class);
    }

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "string_secrets": {
                 "test.setting": "%s"
             }
        }""";

    private static String malformedJSON = """
        {
             "metadata": {
        }""";

    private static String badSettingJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "string_secrets": {
                 "test.bad.setting": "foo"
             }
        }""";

    public void testSettingsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(dataOnlyNode());
        FileSecureSettingsService dataFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSecureSettingsService masterFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, savedClusterState.secretsVersion, testJSON, "foo");
        assertClusterStateSaveOK(savedClusterState.countDownLatch(), masterNode, savedClusterState.metadataVersion(), "foo");

        for (PluginsService ps : internalCluster().getInstances(PluginsService.class)) {
            TestSecureSettingPlugin plugin = (TestSecureSettingPlugin) ps.pluginMap().get(TestSecureSettingPlugin.class.getName());
            assertTrue(plugin.latch.await(20, TimeUnit.SECONDS));
        }
    }

    public void testMalformedSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(dataOnlyNode());
        FileSecureSettingsService dataFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, -1);

        FileSecureSettingsService masterFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, savedClusterState.secretsVersion, malformedJSON, "foo");
        assertClusterErrorStateSaveOK(
            savedClusterState.countDownLatch(),
            savedClusterState.metadataVersion(),
            "[locally_mounted_secrets] failed to parse field [metadata]"
        );
    }

    public void testFixInvalidSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(dataOnlyNode());
        FileSecureSettingsService dataFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSecureSettingsService masterFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, savedClusterState.secretsVersion, badSettingJSON, "foo");
        assertClusterErrorStateSaveOK(
            savedClusterState.countDownLatch(),
            savedClusterState.metadataVersion(),
            "unknown secure setting [test.bad.setting] did you mean [test.setting]?"
        );

        // reset cluster state listener
        savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
        writeJSONFile(masterNode, savedClusterState.secretsVersion, testJSON, "foo");
        assertClusterStateSaveOK(savedClusterState.countDownLatch(), masterNode, savedClusterState.metadataVersion(), "foo");
    }

    // test valid -> invalid
    public void testInvalidSettingsAfterExistingSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(dataOnlyNode());
        FileSecureSettingsService dataFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSecureSettingsService masterFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, savedClusterState.secretsVersion, testJSON, "foo");
        assertClusterStateSaveOK(savedClusterState.countDownLatch(), masterNode, savedClusterState.metadataVersion(), "foo");

        for (PluginsService ps : internalCluster().getInstances(PluginsService.class)) {
            TestSecureSettingPlugin plugin = (TestSecureSettingPlugin) ps.pluginMap().get(TestSecureSettingPlugin.class.getName());
            assertTrue(plugin.latch.await(20, TimeUnit.SECONDS));
        }

        savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
        writeJSONFile(masterNode, savedClusterState.secretsVersion, badSettingJSON, "foo");

        assertClusterErrorStateSaveOK(
            savedClusterState.countDownLatch(),
            savedClusterState.metadataVersion(),
            "unknown secure setting [test.bad.setting] did you mean [test.setting]"
        );
    }

    public void testIncrementSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(dataOnlyNode());
        FileSecureSettingsService dataFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSecureSettingsService masterFileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, savedClusterState.secretsVersion, testJSON, "foo");
        assertClusterStateSaveOK(savedClusterState.countDownLatch(), masterNode, savedClusterState.metadataVersion(), "foo");

        for (PluginsService ps : internalCluster().getInstances(PluginsService.class)) {
            TestSecureSettingPlugin plugin = (TestSecureSettingPlugin) ps.pluginMap().get(TestSecureSettingPlugin.class.getName());
            assertTrue(plugin.latch.await(20, TimeUnit.SECONDS));
            plugin.latch = new CountDownLatch(1);
        }

        savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
        writeJSONFile(masterNode, savedClusterState.secretsVersion, testJSON, "bar");
        // reset cluster state listener
        assertClusterStateSaveOK(savedClusterState.countDownLatch(), masterNode, savedClusterState.metadataVersion(), "bar");

        for (PluginsService ps : internalCluster().getInstances(PluginsService.class)) {
            TestSecureSettingPlugin plugin = (TestSecureSettingPlugin) ps.pluginMap().get(TestSecureSettingPlugin.class.getName());
            assertTrue(plugin.latch.await(20, TimeUnit.SECONDS));
        }
    }

    private void assertClusterErrorStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion, String expectedValue)
        throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.MINUTES);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get()))
            .actionGet();
        assertThat(clusterStateResponse.getState().custom(ClusterStateSecrets.TYPE), nullValue());

        assertThat(clusterStateResponse.getState().custom(ClusterStateSecretsMetadata.TYPE), not(nullValue()));
        ClusterStateSecretsMetadata metadata = clusterStateResponse.getState().custom(ClusterStateSecretsMetadata.TYPE);
        assertThat(metadata.getErrorStackTrace().get(0), containsString(expectedValue));
    }

    private void assertClusterStateSaveOK(
        CountDownLatch savedClusterState,
        String masterNode,
        AtomicLong metadataVersion,
        String expectedValue
    ) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(1, TimeUnit.MINUTES);
        assertTrue(awaitSuccessful);

        // secure settings should not be visible in client requests
        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get()))
            .actionGet();
        assertThat(clusterStateResponse.getState().custom(ClusterStateSecrets.TYPE), nullValue());

        // but it will be visible directly from the cluster service?
        var clusterService = internalCluster().clusterService(masterNode);
        var clusterStateSecrets = clusterService.state().<ClusterStateSecrets>custom(ClusterStateSecrets.TYPE);
        assertThat(clusterStateSecrets, not(nullValue()));
        assertThat(clusterStateSecrets.getVersion(), equalTo(versionCounter.get()));
        assertThat(clusterStateSecrets.getSettings().getString("test.setting"), equalTo(expectedValue));
    }

    private record ServicesAndListeners(CountDownLatch countDownLatch, AtomicLong metadataVersion, long secretsVersion) {

    }

    private ServicesAndListeners setupClusterStateListener(String node, long targetVersion) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ClusterStateSecretsMetadata secureSettingsMetadata = event.state().custom(ClusterStateSecretsMetadata.TYPE);
                if (secureSettingsMetadata != null) {
                    if (secureSettingsMetadata.getVersion() != targetVersion) {
                        return;
                    }
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new ServicesAndListeners(savedClusterState, metadataVersion, targetVersion);
    }

    private long writeJSONFile(String node, long version, String json, String settingValue) throws Exception {

        FileSecureSettingsService fileSettingsService = internalCluster().getInstance(FileSecureSettingsService.class, node);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, Strings.format(json, version, settingValue));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
        logger.info("--> New file settings: [{}]", Strings.format(json, version, settingValue));

        return version;
    }

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).execute().actionGet().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }

    public static class TestSecureSettingPlugin extends Plugin implements ReloadablePlugin {
        volatile CountDownLatch latch = new CountDownLatch(1);

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(SecureSetting.secureString("test.setting", null));
        }

        @Override
        public void reload(Settings settings) throws Exception {
            latch.countDown();
        }
    }
}
