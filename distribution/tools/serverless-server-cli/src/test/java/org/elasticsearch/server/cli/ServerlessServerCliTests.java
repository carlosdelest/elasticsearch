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

package org.elasticsearch.server.cli;

import co.elastic.elasticsearch.serverless.buildinfo.ServerlessBuildExtension;
import joptsimple.OptionSet;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class ServerlessServerCliTests extends CommandTestCase {

    private Path defaultSettingsFile;
    private Path cgroupFs;
    private Path cpuShares;
    private int availableProcessors;
    private final MockServerlessProcess mockServer = new MockServerlessProcess();

    @Before
    public void setupMockConfig() throws IOException {
        Files.createFile(configDir.resolve("log4j2.properties"));
        defaultSettingsFile = configDir.resolve("serverless-default-settings.yml");
        Files.writeString(defaultSettingsFile, "");
        cgroupFs = createTempDir();
        cpuShares = cgroupFs.resolve("cpu/cpu.shares");
        Files.createDirectories(cpuShares.getParent());
        Files.writeString(cpuShares, "1024\n"); // mimic the extra whitespace that may appear in the cgroup fs files
        availableProcessors = 2;
    }

    public void testDefaultsOverridden() throws Exception {
        Files.writeString(defaultSettingsFile, """
            foo.bar: a-default
            """);
        execute("-E", "foo.bar=override");
        assertThat(mockServer.args.nodeSettings().get("foo.bar"), equalTo("override"));
        assertThat(terminal.getOutput(), containsString("Serverless default for [foo.bar] is overridden to [override]"));
    }

    /**
     * Tests that the CLI loads the correct extension for {@link org.elasticsearch.Build}, so that Build information is consistent with
     * the one loaded by the cluster/server process. This is important e.g. to generate the correct command line, see
     * {@link APMJvmOptions} for example.
     */
    public void testServerlessBuildExtensionLoaded() {
        var serverlessExtensionBuild = new ServerlessBuildExtension().getCurrentBuild();
        assertSame(serverlessExtensionBuild, Build.current());
    }

    public void testOvercommitDefaults() throws Exception {
        executeOvercommit(1.0);
        // defaults to no overcommit
        assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("1.0"));

        availableProcessors = 1;
        executeOvercommit(1.0);
        assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("1.0"));
    }

    public void testOvercommit() throws Exception {
        // 512 share * 1.5 = 768 / 1024 = 0.75
        Files.writeString(cpuShares, "512\n");
        executeOvercommit(1.5);
        assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("0.75"));

        // 512 share * 4 = 2048 / 1024 = 2.0
        executeOvercommit(4.0);
        assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("2.0"));
    }

    public void testOvercommitIgnoredOutsideLinux() throws Exception {
        sysprops.put("os.name", "MacOS");
        executeOvercommit(1.0);
        assertThat(mockServer.args.nodeSettings().hasValue(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), is(false));
    }

    public void testOvercommitErrorNodeProcessorsExists() {
        sysprops.put(ServerlessServerCli.PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP, Double.toString(1.0));
        var e = expectThrows(IllegalStateException.class, () -> execute("-E", "node.processors=2"));
        assertThat(e.getMessage(), containsString("node.processors must not be present"));
    }

    public void testOvercommitErrorCpuSharesMissing() throws Exception {
        Files.delete(cpuShares);
        var e = expectThrows(IllegalStateException.class, () -> executeOvercommit(1.0));
        assertThat(e.getMessage(), containsString("cgroups v1 cpu.shares must be set in serverless"));
    }

    public void testOvercommitCappedByAvailable() throws Exception {
        Files.writeString(cpuShares, "2049\n");
        executeOvercommit(1.0);
        assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("2.0"));
        assertThat(terminal.getOutput(), containsString("Capping cpu overcommit to (2)."));
    }

    private void executeOvercommit(double overcommit) throws Exception {
        sysprops.put(ServerlessServerCli.PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP, Double.toString(overcommit));
        execute();
    }

    public void testMoveDiagnosticsExitCodeOKNoOp() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(0, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        try (var targetPathFiles = Files.list(targetPath)) {
            assertThat(targetPathFiles.toList(), is(empty()));
        }
    }

    public void testMoveDiagnosticsNoDumpFileNoOp() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        try (var targetPathFiles = Files.list(targetPath)) {
            assertThat(targetPathFiles.toList(), is(empty()));
        }
    }

    public void testMoveDiagnosticsOneDumpFileCreatesZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile = heapDumpDataPath.resolve("mock.hprof");
        Files.writeString(mockDumpFile, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip.get(0), equalTo("/mock.hprof"));
    }

    public void testMoveDiagnosticsZipFileName() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile = heapDumpDataPath.resolve("mock.hprof");
        Files.writeString(mockDumpFile, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            var settings = Settings.builder().put("serverless.project_id", "PRJ_ID").put("node.name", "NODE_NAME").build();
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, settings, terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].getFileName().toString(), startsWith("PRJ_ID_NODE_NAME"));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip.get(0), equalTo("/mock.hprof"));
    }

    public void testMoveDiagnosticsTwoDumpFilesInZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        var mockDumpFile2 = heapDumpDataPath.resolve("mock2.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");
        Files.writeString(mockDumpFile2, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(2));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/mock2.hprof"));
    }

    public void testMoveDiagnosticsIncludeLogsInZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        var logFile1 = logsDir.resolve("log1.log");
        Files.writeString(logFile1, "log line");
        Files.createDirectory(logsDir.resolve("sub/"));
        var logFile2 = logsDir.resolve("sub/log2.json");
        Files.writeString(logFile2, "{}");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(3));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/logs/log1.log", "/logs/sub/log2.json"));
    }

    public void testMoveDiagnosticsIncludeReplayLogsInZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();
        Path replayDir = createEmptyTempDir();

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        var replayFile = replayDir.resolve("replay_1234.log");
        Files.writeString(replayFile, "Some replay content");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, replayDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(2));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/replay_1234.log"));
    }

    public void testMoveDiagnosticsIncludeReplayLogsInZipOnce() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        var replayFile = logsDir.resolve("replay_1234.log");
        Files.writeString(replayFile, "Some replay content");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(2));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/logs/replay_1234.log"));
    }

    public void testMoveDiagnosticsInvalidDumpDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = Path.of("/not/existing/");
        Path logsDir = Path.of("/not/existing/logs/");

        try (var serverlessCli = new ServerlessServerCli()) {
            expectThrows(
                UserException.class,
                () -> serverlessCli.moveDiagnosticsToTargetPath(
                    3,
                    targetPath,
                    heapDumpDataPath,
                    logsDir,
                    logsDir,
                    emptySettings(),
                    terminal
                )
            );
        }
        var targetPathFiles = FileSystemUtils.files(targetPath);
        assertThat(targetPathFiles, emptyArray());
    }

    public void testMoveDiagnosticsInvalidLogsDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = Path.of("/not/existing/logs/");

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip, contains("/mock1.hprof"));

        assertThat(
            terminal.getErrorOutput(),
            containsString("/not/existing/logs is not a valid directory. The diagnostic file will not contain log files.")
        );
    }

    public void testMoveDiagnosticsInvalidReplayDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();
        Path replayDir = Path.of("/not/existing/replays/");

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, replayDir, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        assertThat(targetPathFiles.length, is(1));
        assertThat(targetPathFiles[0].toString(), endsWith(".zip"));

        List<String> filesInZip = getFilesInZip(targetPathFiles[0]);

        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip, contains("/mock1.hprof"));

        assertThat(
            terminal.getErrorOutput(),
            containsString("/not/existing/replays is not a valid directory. The diagnostic file will not contain replay files.")
        );
    }

    public void testOnExitActionInitializationNoDumpArgNoOp() {
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(Map.of(), createMockServerArgs(), List.of(), terminal);
            assertThat(serverlessCli.onExitDiagnosticsAction, is(ServerlessServerCli.NO_OP_EXIT_ACTION));
        }
    }

    public void testOnExitActionInitializationNoTargetDirNoOp() {
        var invalidTargetDir = createTempDir().resolve("NO_PATH");
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, invalidTargetDir.toString()),
                createMockServerArgs(),
                List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(ServerlessServerCli.NO_OP_EXIT_ACTION));
        }
    }

    public void testOnExitActionInitializationValid() {
        var validTargetDir = createTempDir();
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
                createMockServerArgs(),
                List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(not(ServerlessServerCli.NO_OP_EXIT_ACTION)));
        }
    }

    private static Settings emptySettings() {
        return Settings.builder().build();
    }

    private static ServerArgs createMockServerArgs() {
        return new ServerArgs(false, false, null, KeyStoreWrapper.create(), null, null, null);
    }

    private static Path createEmptyTempDir() throws IOException {
        Path path = createTempDir();
        IOUtils.rm(FileSystemUtils.files(path));
        return path;
    }

    private static List<String> getFilesInZip(Path zipFile) throws IOException {
        List<String> filesInZip = new ArrayList<>();
        try (var zipFileSystem = FileSystems.newFileSystem(zipFile)) {
            zipFileSystem.getRootDirectories().forEach(root -> {
                try (var files = Files.walk(root)) {
                    files.forEach(path -> {
                        if (Files.isDirectory(path) == false) {
                            // Skip ExtraFS (optional) special file marker
                            if (path.endsWith("extra0") == false) {
                                filesInZip.add(path.toString());
                            }
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return filesInZip;
    }

    private class MockServerlessProcess extends ServerProcess {
        ServerArgs args;

        MockServerlessProcess() {
            super(null, null);
        }

        @Override
        public long pid() {
            return 12345;
        }

        @Override
        public void detach() {}

        @Override
        public int waitFor() {
            return 0;
        }

        @Override
        public void stop() {}
    }

    @Override
    protected Command newCommand() {
        return new ServerlessServerCli() {
            @Override
            protected Command loadTool(String toolname, String libs) {
                throw new AssertionError("tests shoudln't be loading tools");
            }

            @Override
            Environment autoConfigureSecurity(
                Terminal terminal,
                OptionSet options,
                ProcessInfo processInfo,
                Environment env,
                SecureString keystorePassword
            ) throws Exception {
                return env;
            }

            @Override
            protected ServerProcess startServer(Terminal terminal, ProcessInfo processInfo, ServerArgs args) {
                mockServer.args = args;
                return mockServer;
            }

            @Override
            void syncPlugins(Terminal terminal, Environment env, ProcessInfo processInfo) throws Exception {}

            @Override
            protected Path getCgroupFs() {
                return cgroupFs;
            }

            @Override
            protected int getAvailableProcessors() {
                return availableProcessors;
            }
        };
    }
}
