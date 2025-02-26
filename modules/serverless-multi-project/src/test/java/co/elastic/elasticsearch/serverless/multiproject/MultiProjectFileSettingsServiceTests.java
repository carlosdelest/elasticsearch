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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.reservedstate.service.ReservedStateVersionCheck;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MultiProjectFileSettingsServiceTests extends ESTestCase {

    private Environment env;
    private ClusterService clusterService;
    private ReservedClusterStateService controller;
    private ThreadPool threadpool;
    private MultiProjectFileSettingsService fileSettingsService;
    private FileSettingsHealthIndicatorService healthIndicatorService;
    private Path settingsFile;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadpool = new TestThreadPool("multi_project_file_settings_service_tests");

        clusterService = new ClusterService(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "test").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool,
            new TaskManager(Settings.EMPTY, threadpool, Set.of())
        );

        DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();

        clusterService.setNodeConnectionsService(mock(NodeConnectionsService.class));
        clusterService.getClusterApplierService().setInitialState(clusterState);
        clusterService.getMasterService().setClusterStatePublisher((e, pl, al) -> {
            ClusterServiceUtils.setAllElapsedMillis(e);
            al.onCommit(TimeValue.ZERO);
            for (DiscoveryNode node : e.getNewState().nodes()) {
                al.onNodeAck(node, null);
            }
            pl.onResponse(null);
        });
        clusterService.getMasterService().setClusterStateSupplier(() -> clusterState);
        env = newEnvironment(Settings.EMPTY);

        Files.createDirectories(env.configDir());

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        controller = spy(
            new ReservedClusterStateService(
                clusterService,
                mock(RerouteService.class),
                List.of(new ReservedClusterSettingsAction(clusterSettings)),
                List.of()
            )
        );
        healthIndicatorService = spy(new FileSettingsHealthIndicatorService(Settings.EMPTY));
        fileSettingsService = spy(new MultiProjectFileSettingsService(clusterService, controller, env, healthIndicatorService));
        settingsFile = fileSettingsService.watchedFile();
    }

    @After
    public void tearDown() throws Exception {
        if (fileSettingsService.lifecycleState() == Lifecycle.State.STARTED) {
            logger.info("Stopping file settings service");
            fileSettingsService.stop();
        }
        if (fileSettingsService.lifecycleState() == Lifecycle.State.STOPPED) {
            logger.info("Closing file settings service");
            fileSettingsService.close();
        }

        super.tearDown();
        clusterService.close();
        threadpool.shutdownNow();
    }

    private static ArgumentMatcher<Path> nonExtraFsFile() {
        // sometimes files from ExtraFS appear in the directory
        return p -> p.getFileName().toString().startsWith("extra") == false;
    }

    public void testStartStop() {
        fileSettingsService.start();
        assertFalse(fileSettingsService.watching());
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileSettingsService.watching());
        fileSettingsService.stop();
        assertFalse(fileSettingsService.watching());
        verify(healthIndicatorService, times(1)).startOccurred();
        verify(healthIndicatorService, times(1)).stopOccurred();
    }

    public void testOperatorDirName() {
        Path operatorPath = fileSettingsService.watchedFileDir();
        assertTrue(operatorPath.startsWith(env.configDir()));
        assertTrue(operatorPath.endsWith("operator"));

        Path operatorSettingsFile = fileSettingsService.watchedFile();
        assertTrue(operatorSettingsFile.startsWith(operatorPath));
        assertTrue(operatorSettingsFile.endsWith("settings.json"));
    }

    @SuppressWarnings("unchecked")
    public void testInitialFileError() throws Exception {
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(new IllegalStateException("Some exception"));
            return null;
        }).when(controller).process(any(), any(XContentParser.class), eq(randomFrom(ReservedStateVersionCheck.values())), any());

        Answer<?> checkExecute = i -> {
            i.callRealMethod();  // should throw an exception
            fail(i.getMethod().getName() + " should have thrown an exception");
            return null;
        };
        doAnswer(checkExecute).when(fileSettingsService).processInitialFilesMissing();
        doAnswer(checkExecute).when(fileSettingsService).processFile(eq(settingsFile), eq(false));

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(countDownOnInvoke(latch)).when(fileSettingsService).processFile(eq(settingsFile), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(settingsFile, "{}");

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        // wait until the watcher thread has started, and it has discovered the file
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(fileSettingsService, times(1)).processFile(eq(settingsFile), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());

        assertEquals(YELLOW, healthIndicatorService.calculate(false, null).status());
        verify(healthIndicatorService, times(1)).changeOccurred();
        verify(healthIndicatorService, times(1)).failureOccurred(argThat(s -> s.startsWith(IllegalStateException.class.getName())));
    }

    @SuppressWarnings("unchecked")
    public void testInitialFilesWorks() throws Exception {
        // Let's check that if we didn't throw an error that everything works
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(4)).accept(null);
            return null;
        }).when(controller).process(any(), any(), any(XContentParser.class), any(), any());

        final int projectNum = 3;

        CountDownLatch processFileLatch = new CountDownLatch(2 * projectNum + 1);
        doAnswer(countDownOnInvoke(processFileLatch)).when(fileSettingsService).processFile(argThat(nonExtraFsFile()), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        for (int p = 0; p < projectNum; p++) {
            writeProjectFile(Integer.toString(p), "{}");
            writeProjectSecretsFile(Integer.toString(p), "{}");
        }
        writeTestFile(settingsFile, settingsFileForProjects(projectNum));

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileLatch);

        // 2 file writes per project (secrets + config) and the global settings file
        verify(fileSettingsService, times(2 * projectNum + 1)).processFile(argThat(nonExtraFsFile()), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());
        verify(controller, times(2 * projectNum)).process(
            any(),
            any(),
            any(XContentParser.class),
            eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION),
            any()
        );

        assertEquals(GREEN, healthIndicatorService.calculate(false, null).status());
        verify(healthIndicatorService, times(1)).changeOccurred();
        verify(healthIndicatorService, times(2 * projectNum + 1)).successOccurred();
    }

    @SuppressWarnings("unchecked")
    public void testProcessFileChanges() throws Exception {
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());

        CountDownLatch processFileCreationLatch = new CountDownLatch(1);
        doAnswer(countDownOnInvoke(processFileCreationLatch)).when(fileSettingsService).processFile(eq(settingsFile), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        // contents of the JSON don't matter, we just need a file to exist
        writeTestFile(settingsFile, "{}");

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileCreationLatch);

        CountDownLatch processFileChangeLatch = new CountDownLatch(1);
        doAnswer(countDownOnInvoke(processFileChangeLatch)).when(fileSettingsService).processFile(eq(settingsFile), eq(false));

        verify(fileSettingsService, times(1)).processFile(eq(settingsFile), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());

        // Touch the file to get an update
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(settingsFile, FileTime.from(now));

        longAwait(processFileChangeLatch);

        verify(fileSettingsService, times(1)).processFile(eq(settingsFile), eq(false));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_VERSION_ONLY), any());

        assertEquals(GREEN, healthIndicatorService.calculate(false, null).status());
        verify(healthIndicatorService, times(2)).changeOccurred();
        verify(healthIndicatorService, times(2)).successOccurred();
    }

    @SuppressWarnings("unchecked")
    public void testNewProjectFile() throws Exception {
        BlockingQueue<Object> processSettings = new ArrayBlockingQueue<>(5);
        BlockingQueue<ProjectId> processProject = new ArrayBlockingQueue<>(5);
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            processSettings.add(new Object());
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(4)).accept(null);
            processProject.add(i.getArgument(0));
            return null;
        }).when(controller).process(any(), any(), any(XContentParser.class), any(), any());

        // initial file
        CountDownLatch processFileCreationLatch = new CountDownLatch(1);
        doAnswer(countDownOnInvoke(processFileCreationLatch)).when(fileSettingsService).processFile(eq(settingsFile), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        writeTestFile(settingsFile, settingsFileForProjects(0));

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileCreationLatch);
        assertThat(longPoll(processSettings), notNullValue());

        // create settings file for project 1
        writeTestFile(projectFile("1"), "{}");
        writeTestFile(projectSecretsFile("1"), "{}");
        assertThat(processSettings, empty());
        assertThat(processProject, empty());

        // register project 0
        writeTestFile(settingsFile, settingsFileForProjects(1));
        assertThat(longPoll(processSettings), notNullValue());
        assertThat(processProject, empty());

        // create the settings files for project 0 - this triggers project 0 creation
        writeTestFile(projectFile("0"), "{}");
        writeTestFile(projectSecretsFile("0"), "{}");
        assertThat(processSettings, empty());
        // Process secrets and config for project 0
        assertThat(longPoll(processProject), equalTo(new ProjectId("0")));
        assertThat(longPoll(processProject), equalTo(new ProjectId("0")));

        // register project 1 (and 0) - this triggers project 1 creation
        writeTestFile(settingsFile, settingsFileForProjects(2));
        assertThat(longPoll(processSettings), notNullValue());
        // Process secrets and config for project 1
        assertThat(longPoll(processProject), equalTo(new ProjectId("1")));
        assertThat(longPoll(processProject), equalTo(new ProjectId("1")));
    }

    @SuppressWarnings("unchecked")
    public void testProjectFilesChange() throws Exception {
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(3)).accept(null);
            return null;
        }).when(controller).process(any(), any(XContentParser.class), any(), any());
        doAnswer(i -> {
            ((Consumer<Exception>) i.getArgument(4)).accept(null);
            return null;
        }).when(controller).process(any(), any(), any(XContentParser.class), any(), any());

        final int projectNum = 2;

        CountDownLatch processFileCreationLatch = new CountDownLatch(2 * projectNum + 1);
        doAnswer(countDownOnInvoke(processFileCreationLatch)).when(fileSettingsService).processFile(argThat(nonExtraFsFile()), eq(true));

        Files.createDirectories(fileSettingsService.watchedFileDir());
        for (int p = 0; p < projectNum; p++) {
            writeProjectFile(Integer.toString(p), "{}");
            writeProjectSecretsFile(Integer.toString(p), "{}");
        }
        writeTestFile(settingsFile, settingsFileForProjects(projectNum));

        fileSettingsService.start();
        fileSettingsService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));

        longAwait(processFileCreationLatch);

        verify(fileSettingsService, times(2 * projectNum + 1)).processFile(argThat(nonExtraFsFile()), eq(true));
        verify(controller, times(1)).process(any(), any(XContentParser.class), eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION), any());
        verify(controller, times(2 * projectNum)).process(
            any(),
            any(),
            any(XContentParser.class),
            eq(ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION),
            any()
        );

        // Touch a project file to update it
        Path projectFile = projectFile(Integer.toString(0));

        CountDownLatch processFileChangeLatch = new CountDownLatch(1);
        doAnswer(countDownOnInvoke(processFileChangeLatch)).when(fileSettingsService).processFile(eq(projectFile), eq(false));

        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(projectFile, FileTime.from(now));

        longAwait(processFileChangeLatch);

        verify(fileSettingsService, times(1)).processFile(eq(projectFile), eq(false));
        // Process once for config and once for secrets
        verify(controller, times(2)).process(
            any(),
            any(),
            any(XContentParser.class),
            eq(ReservedStateVersionCheck.HIGHER_VERSION_ONLY),
            any()
        );

        // TODO: health outputs
    }

    private static Answer<?> countDownOnInvoke(CountDownLatch latch) {
        return i -> {
            try {
                return i.callRealMethod();
            } finally {
                latch.countDown();
            }
        };
    }

    private static String settingsFileForProjects(int projectNum) {
        if (projectNum == 0) {
            return "{\"projects\":[]}";
        }
        StringBuilder projectList = new StringBuilder("[");
        for (int p = 0; p < projectNum; p++) {
            projectList.append('"').append(p).append("\",");
        }
        projectList.setCharAt(projectList.length() - 1, ']');
        return Strings.format("{\"projects\":%s}", projectList);
    }

    private Path projectFile(String projectId) {
        return settingsFile.resolveSibling("project-" + projectId + ".json");
    }

    private Path projectSecretsFile(String projectId) {
        return settingsFile.resolveSibling("project-" + projectId + ".secrets.json");
    }

    private Path writeProjectFile(String projectId, String contents) throws IOException {
        Path path = projectFile(projectId);
        writeTestFile(path, contents);
        return path;
    }

    private Path writeProjectSecretsFile(String projectId, String contents) throws IOException {
        Path path = projectSecretsFile(projectId);
        writeTestFile(path, contents);
        return path;
    }

    private static void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, contents);
        try {
            Files.move(tempFilePath, path, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempFilePath, path, REPLACE_EXISTING);
        }
    }

    // this waits for up to 20 seconds to account for watcher service differences between OSes
    // on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
    // on Linux is instantaneous. Windows is instantaneous too.
    private static void longAwait(CountDownLatch latch) {
        try {
            assertTrue("longAwait: CountDownLatch did not reach zero within the timeout", latch.await(20, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "longAwait: interrupted waiting for CountDownLatch to reach zero");
        }
    }

    private static <T> T longPoll(BlockingQueue<T> queue) {
        try {
            return queue.poll(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("longPoll: interrupted waiting for BlockingQueue poll", e);
        }
    }
}
