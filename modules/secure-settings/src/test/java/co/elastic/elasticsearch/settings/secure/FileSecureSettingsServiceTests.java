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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileSecureSettingsServiceTests extends ESTestCase {

    private static final String WITH_STRING_SECRET = """
        {
            "metadata": {
                "version": "3",
                "compatibility": "8.9.0"
            },
            "string_secrets": {
                "foo": "baz"
            }
        }
        """;

    private static final String INVALID_KEY_METADATA = """
        {
            "invalid_key": {}
            "metadata": {
                "version": "3",
                "compatibility": "8.9.0"
            },
            "string_secrets": {}
        }
        """;

    private Environment env;
    private ClusterService clusterService;
    private MasterServiceTaskQueue<ClusterStateTaskListener> updateQueue;
    private MasterServiceTaskQueue<ClusterStateTaskListener> errorQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.env = newEnvironment();
        Files.createDirectories(env.configDir().resolve(LocallyMountedSecrets.SECRETS_DIRECTORY));
        this.clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        this.updateQueue = getMockQueue();
        this.errorQueue = getMockQueue();
        when(clusterService.createTaskQueue(eq("secure_settings"), any(), any())).thenReturn(updateQueue);
        when(clusterService.createTaskQueue(eq("secure_settings_errors"), any(), any())).thenReturn(errorQueue);
    }

    public void testDirectoryName() {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);
        Path secureSettingsDir = fileSecureSettingsService.watchedFileDir();
        assertTrue(secureSettingsDir.startsWith(env.configDir()));
        assertTrue(secureSettingsDir.endsWith("secrets"));
    }

    public void testFileName() {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);
        Path secureSettingsFile = fileSecureSettingsService.watchedFile();
        assertTrue(secureSettingsFile.startsWith(env.configDir()));
        assertTrue(secureSettingsFile.endsWith("secrets.json"));
    }

    public void testValidSetting() throws Exception {
        // we need the secure setting defined to pass validation
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(
                SecureSetting.secureString("foo", null)
            )));

        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);
        writeTestFile(fileSecureSettingsService.watchedFile(), WITH_STRING_SECRET);

        fileSecureSettingsService.processFileChanges();

        ArgumentCaptor<ClusterStateTaskListener> taskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(updateQueue, times(1)).submitTask(any(), taskCapture.capture(), any());

        assertThat(taskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata metadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) taskCapture.getValue()).getMetadata();
        assertTrue(metadata.isSuccess());
        assertThat(metadata.getVersion(), equalTo(3L));

        assertReprocessedFileNotQueued(fileSecureSettingsService, metadata, 1, 0);
    }

    public void testInvalidSetting() throws Exception {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);

        writeTestFile(fileSecureSettingsService.watchedFile(), WITH_STRING_SECRET);
        fileSecureSettingsService.processFileChanges();

        ArgumentCaptor<ClusterStateTaskListener> taskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(errorQueue, times(1)).submitTask(any(), taskCapture.capture(), any());

        assertThat(taskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata metadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) taskCapture.getValue()).getMetadata();
        assertFalse(metadata.isSuccess());
        assertThat(metadata.getVersion(), equalTo(3L));
        assertThat(
            metadata.getErrorStackTrace().get(0),
            containsString("unknown secure setting [foo] please check that any required plugins are installed")
        );

        assertReprocessedFileNotQueued(fileSecureSettingsService, metadata, 0, 1);
    }

    public void testMalformedJsonParseError() throws Exception {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);

        writeTestFile(fileSecureSettingsService.watchedFile(), "{");
        fileSecureSettingsService.processFileChanges();

        ArgumentCaptor<ClusterStateTaskListener> taskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(errorQueue, times(1)).submitTask(any(), taskCapture.capture(), any());

        assertThat(taskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata metadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) taskCapture.getValue()).getMetadata();
        assertFalse(metadata.isSuccess());
        assertThat(metadata.getVersion(), equalTo(-1L));
        assertThat(metadata.getErrorStackTrace().get(0), containsString("Unexpected end-of-input: expected close marker for Object"));

        assertReprocessedFileNotQueued(fileSecureSettingsService, metadata, 0, 1);
    }

    // test invalid key error
    public void testInvalidJsonKeyParseError() throws Exception {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);

        writeTestFile(fileSecureSettingsService.watchedFile(), INVALID_KEY_METADATA);
        fileSecureSettingsService.processFileChanges();

        ArgumentCaptor<ClusterStateTaskListener> taskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(errorQueue, times(1)).submitTask(any(), taskCapture.capture(), any());

        assertThat(taskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata metadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) taskCapture.getValue()).getMetadata();
        assertFalse(metadata.isSuccess());
        assertThat(metadata.getVersion(), equalTo(-1L));
        assertThat(metadata.getErrorStackTrace().get(0), containsString("[locally_mounted_secrets] unknown field [invalid_key]"));

        assertReprocessedFileNotQueued(fileSecureSettingsService, metadata, 0, 1);
    }

    public void testClusterStateApplicationError() throws Exception {
        // we need the secure setting defined to pass validation
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(
                SecureSetting.secureString("foo", null)
            )));

        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, env);

        writeTestFile(fileSecureSettingsService.watchedFile(), WITH_STRING_SECRET);

        // process the task
        fileSecureSettingsService.processFileChanges();

        ArgumentCaptor<ClusterStateTaskListener> updateTaskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(updateQueue, times(1)).submitTask(any(), updateTaskCapture.capture(), any());

        assertThat(updateTaskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata updateMetadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) updateTaskCapture.getValue())
            .getMetadata();
        assertTrue(updateMetadata.isSuccess());
        assertThat(updateMetadata.getVersion(), equalTo(3L));

        // assert that there's nothing on the error queue
        verify(errorQueue, never()).submitTask(any(), any(), any());

        // invoke the failure to put an error on the error queue
        updateTaskCapture.getValue().onFailure(new IllegalStateException("some problem"));

        ArgumentCaptor<ClusterStateTaskListener> errorTaskCapture = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        verify(errorQueue, times(1)).submitTask(any(), errorTaskCapture.capture(), any());

        assertThat(errorTaskCapture.getValue(), instanceOf(FileSecureSettingsService.MetadataHoldingUpdateTask.class));

        ClusterStateSecretsMetadata errorMetadata = ((FileSecureSettingsService.MetadataHoldingUpdateTask) errorTaskCapture.getValue())
            .getMetadata();
        assertFalse(errorMetadata.isSuccess());
        assertThat(errorMetadata.getVersion(), equalTo(3L));
        assertThat(errorMetadata.getErrorStackTrace().get(0), containsString("some problem"));
    }

    // this method helps us check that we aren't going to submit cluster state updates
    // when the file on disk would give us the same metadata version
    private void assertReprocessedFileNotQueued(
        FileSecureSettingsService fileSecureSettingsService,
        ClusterStateSecretsMetadata metadata,
        int updateQueueEvents,
        int errorQueueEvents) throws InterruptedException, ExecutionException, IOException {
        // simulate this metadata in cluster state
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .putCustom(ClusterStateSecretsMetadata.TYPE, metadata)
                .build()
        );
        fileSecureSettingsService.processFileChanges();
        verify(updateQueue, times(updateQueueEvents)).submitTask(any(), any(), any());
        verify(errorQueue, times(errorQueueEvents)).submitTask(any(), any(), any());
    }

    // helpers
    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, contents.getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }

    @SuppressWarnings("unchecked")
    private <T extends ClusterStateTaskListener> MasterServiceTaskQueue<T> getMockQueue() {
        return mock(MasterServiceTaskQueue.class);
    }
}
