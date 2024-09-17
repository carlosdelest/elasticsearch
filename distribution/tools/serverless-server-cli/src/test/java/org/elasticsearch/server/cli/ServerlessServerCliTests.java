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
import junit.framework.AssertionFailedError;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.server.cli.ProcessUtil.nonInterruptibleVoid;
import static org.elasticsearch.server.cli.ServerlessServerCli.DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class ServerlessServerCliTests extends CommandTestCase {

    private Path defaultSettingsFile;
    private Path cgroupFs;
    private String cgroupV2DirName;
    private Collection<CpuControlFile> cpuControlFiles;
    private int availableProcessors;
    private final MockServerlessProcess mockServer = new MockServerlessProcess();

    /**
     * Allows us to test both {@code cpu.shares} and {@code cpu.weight}
     * @param file (you can assume the parent directory already exists at the start of the test)
     * @param denominator the amount that represents one full CPU
     * @param controllersFile is how we tell whether it's v1 or v2; if this file exists, it's v2
     */
    record CpuControlFile(Path file, int denominator, Path controllersFile) {
        void setup() throws IOException {
            if (controllersFile != null) {
                Files.writeString(controllersFile, "cpuset cpu io memory hugetlb pids rdma");
            }
        }

        void cleanup() throws IOException {
            Files.deleteIfExists(file);
            if (controllersFile != null) {
                Files.deleteIfExists(controllersFile);
            }
        }
    }

    @Before
    public void setupMockConfig() throws IOException {
        Files.createFile(configDir.resolve("log4j2.properties"));
        defaultSettingsFile = configDir.resolve("serverless-default-settings.yml");
        Files.writeString(defaultSettingsFile, "");

        cgroupFs = createTempDir();
        CpuControlFile cpuShares = new CpuControlFile(cgroupFs.resolve("cpu/cpu.shares"), 1024, null);
        cgroupV2DirName = "test-cgroup-name";
        var cgroupV2Dir = cgroupFs.resolve(cgroupV2DirName);
        CpuControlFile cpuWeight = new CpuControlFile(cgroupV2Dir.resolve("cpu.weight"), 100, cgroupFs.resolve("cgroup.controllers"));
        Files.createDirectories(cpuShares.file().getParent());
        Files.createDirectories(cgroupV2Dir);

        cpuControlFiles = List.of(cpuShares, cpuWeight);
        availableProcessors = 2;
        mockServer.waitForAction = null;
        mockServer.stopAction = null;
        mockServer.forceStopAction = null;
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
        for (var cpu : cpuControlFiles) {
            try {
                cpu.setup();

                Files.writeString(cpu.file(), cpu.denominator + "\n"); // mimic the extra whitespace that may appear in the cgroup fs files
                executeOvercommit(1.0);
                // defaults to no overcommit
                assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("1.0"));

                availableProcessors = 1;
                executeOvercommit(1.0);
                assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("1.0"));
            } finally {
                cpu.cleanup();
            }
        }
    }

    public void testOvercommit() throws Exception {
        for (var cpu : cpuControlFiles) {
            try {
                cpu.setup();

                // Half a CPU * 1.5 = 0.75
                Files.writeString(cpu.file(), cpu.denominator / 2 + "\n"); // mimic the extra whitespace that may appear in the cgroup fs
                                                                           // files
                executeOvercommit(1.5);
                assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("0.75"));

                // Half a CPU * 4 = 2.0
                executeOvercommit(4.0);
                assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("2.0"));
            } finally {
                cpu.cleanup();
            }
        }
    }

    public void testOvercommitCappedByAvailable() throws Exception {
        for (var cpu : cpuControlFiles) {
            try {
                cpu.setup();

                Files.writeString(cpu.file(), (cpu.denominator * 2 + 1) + "\n"); // Just a little more than 2 CPUs
                executeOvercommit(1.0);
                assertThat(mockServer.args.nodeSettings().get(EsExecutors.NODE_PROCESSORS_SETTING.getKey()), equalTo("2.0"));
                assertThat(terminal.getOutput(), containsString("Capping cpu overcommit to (2)."));
            } finally {
                cpu.cleanup();
            }
        }
    }

    public void testOvercommitErrorCpuFileMissing() throws Exception {
        for (var cpu : cpuControlFiles) {
            try {
                cpu.setup();
                Files.deleteIfExists(cpu.file());
                var e = expectThrows(IllegalStateException.class, () -> executeOvercommit(1.0));
                assertThat(e.getMessage(), both(containsString("cgroups")).and(containsString("must be set in serverless")));
            } finally {
                cpu.cleanup();
            }
        }
    }

    public void testProjectId() throws Exception {
        execute("-E", "serverless.project_id=abc123");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_PROJECT_ID_SETTING), is("abc123"));
    }

    public void testProjectType() throws Exception {
        execute("-E", "serverless.project_type=OBSERVABILITY");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_PROJECT_TYPE_SETTING), is("OBSERVABILITY"));
    }

    public void testNodeTierSettings() throws Exception {
        execute("-E", "node.roles=index");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_NODE_ROLE_SETTING), is("index"));

        execute("-E", "node.roles=search");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_NODE_ROLE_SETTING), is("search"));

        execute("-E", "node.roles=ml");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_NODE_ROLE_SETTING), is("ml"));

        execute("-E", "node.roles=index,search,ml");
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_NODE_ROLE_SETTING), is("index"));

        String nodeTier = randomFrom("index", "search", "ml");
        execute("-E", "node.roles=master,remote_cluster_client," + nodeTier);
        assertThat(mockServer.args.nodeSettings().get(ServerlessServerCli.APM_NODE_ROLE_SETTING), is(nodeTier));
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

    private void executeOvercommit(double overcommit) throws Exception {
        sysprops.put(ServerlessServerCli.PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP, Double.toString(overcommit));
        execute();
    }

    public void testMoveDiagnosticsExitCodeOKNoOp() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(0, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());

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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, settings, terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());

        assertThat(zipFile.get().getFileName().toString(), startsWith("PRJ_ID_NODE_NAME"));
        List<String> filesInZip = getFilesInZip(zipFile.get());

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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);

        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());

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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());

        assertThat(filesInZip, hasSize(3));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/logs/log1.log", "/logs/sub/log2.json"));
    }

    public void testMoveDiagnosticsIncludeReplayLogsInZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        var replayFile = logsDir.resolve("replay_1234.log");
        Files.writeString(replayFile, "Some replay content");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());

        assertThat(filesInZip, hasSize(2));
        assertThat(filesInZip, containsInAnyOrder("/mock1.hprof", "/logs/replay_1234.log"));
    }

    public void testMoveDiagnosticsIncludeFatalErrorLogsInZip() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var fatalErrorFile1 = logsDir.resolve("hs_err_123.log");
        var fatalErrorFile2 = logsDir.resolve("hs_err_234.log");
        Files.writeString(fatalErrorFile1, "Some error content");
        Files.writeString(fatalErrorFile2, "Some other error content");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());

        assertThat(filesInZip, hasSize(2));
        assertThat(filesInZip, containsInAnyOrder("/logs/hs_err_123.log", "/logs/hs_err_234.log"));
    }

    public void testZipChecksumPrintedOnStdout() throws UserException, IOException {
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
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());

        String sha256;
        try (var zipFileStream = Files.newInputStream(zipFile.get())) {
            sha256 = MessageDigests.toHexString(MessageDigests.digest(zipFileStream, MessageDigests.sha256()));
        }

        assertThat(terminal.getOutput(), containsString("SHA256: [" + sha256 + "]"));
    }

    public void testMoveDiagnosticsInvalidDumpDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = Path.of("/not/existing/");
        Path logsDir = Path.of("/not/existing/logs/");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        assertThat(terminal.getOutput(), containsString("No diagnostic files found, skipping post-exit diagnostic collection"));
        var targetPathFiles = FileSystemUtils.files(targetPath);
        assertThat(targetPathFiles, emptyArray());
    }

    public void testMoveDiagnosticsInvalidLogsDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = Path.of("/not/existing/logs/");

        var mockDumpFile = heapDumpDataPath.resolve("mock.hprof");
        Files.writeString(mockDumpFile, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            var settings = Settings.builder().put("serverless.project_id", "PRJ_ID").put("node.name", "NODE_NAME").build();
            var exception = expectThrows(
                UserException.class,
                () -> serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, settings, terminal)
            );

            assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
            assertThat(exception.getCause().getMessage(), containsString(logsDir.toString()));
        }
    }

    public void testOnExitActionInitializationNoDumpArgNoOp() throws IOException {
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(Map.of(), createMockServerArgs(null), List.of(), terminal);
            assertThat(serverlessCli.onExitDiagnosticsAction, is(ServerlessServerCli.NO_OP_EXIT_ACTION));
        }
    }

    public void testOnExitActionInitializationNoTargetDirNoOp() throws IOException {
        var invalidTargetDir = createTempDir().resolve("NO_PATH");
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, invalidTargetDir.toString()),
                createMockServerArgs(null),
                List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(ServerlessServerCli.NO_OP_EXIT_ACTION));
        }
    }

    public void testOnExitActionInitializationInvalidLogsDirNoOp() throws UserException, IOException {
        var validTargetDir = createTempDir();
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
                createMockServerArgs(null),
                List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(ServerlessServerCli.NO_OP_EXIT_ACTION));
        }

        assertThat(
            terminal.getErrorOutput(),
            containsString("The logs path [null] is not a valid directory. The diagnostic bundle will not contain log files.")
        );
    }

    public void testInvalidReplayDir() throws UserException, IOException {
        Path targetPath = createEmptyTempDir();
        Path heapDumpDataPath = createEmptyTempDir();
        Path logsDir = createEmptyTempDir();

        var validTargetDir = createTempDir();
        var validLogsDir = createTempDir();
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
                createMockServerArgs(validLogsDir),
                List.of(
                    ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo",
                    ServerlessServerCli.FATAL_ERROR_LOG_FILE_JVM_OPT + "/not/existing/replays"
                ),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(not(ServerlessServerCli.NO_OP_EXIT_ACTION)));
            assertThat(
                terminal.getErrorOutput(),
                containsString(
                    Strings.format(
                        "The fatal error log path [/not/existing/replays] is not set to the logs path [%s]; "
                            + "fatal error logs will not be added to the diagnostic bundle.",
                        validLogsDir
                    )
                )
            );
        }

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, targetPath, heapDumpDataPath, logsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(targetPath);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());
        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip, contains("/mock1.hprof"));
    }

    public void testInvalidFatalErrorLogsDir() throws UserException, IOException {
        Path heapDumpDataPath = createEmptyTempDir();
        Path fatalErrorLogsDir = createEmptyTempDir();

        var validTargetDir = createTempDir();
        var validLogsDir = createTempDir();
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
                createMockServerArgs(validLogsDir),
                List.of(
                    ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo",
                    ServerlessServerCli.FATAL_ERROR_LOG_FILE_JVM_OPT + fatalErrorLogsDir
                ),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(not(ServerlessServerCli.NO_OP_EXIT_ACTION)));
            assertThat(
                terminal.getErrorOutput(),
                containsString(
                    Strings.format(
                        "The fatal error log path [%s] is not set to the logs path [%s]; "
                            + "fatal error logs will not be added to the diagnostic bundle.",
                        fatalErrorLogsDir,
                        validLogsDir
                    )
                )
            );
        }

        var mockDumpFile1 = heapDumpDataPath.resolve("mock1.hprof");
        Files.writeString(mockDumpFile1, "MOCK-DUMP");

        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.moveDiagnosticsToTargetPath(3, validTargetDir, heapDumpDataPath, validLogsDir, emptySettings(), terminal);
        }

        var targetPathFiles = FileSystemUtils.files(validTargetDir);
        var zipFile = Arrays.stream(targetPathFiles).filter(x -> x.toString().endsWith(".zip")).findFirst();
        assertThat(zipFile, isPresent());
        List<String> filesInZip = getFilesInZip(zipFile.get());
        assertThat(filesInZip, hasSize(1));
        assertThat(filesInZip, contains("/mock1.hprof"));
    }

    public void testOnExitActionInitializationValid() throws IOException {
        var validTargetDir = createTempDir();
        var validLogsDir = createTempDir();
        try (var serverlessCli = new ServerlessServerCli()) {
            serverlessCli.initializeOnExitDiagnosticsAction(
                Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
                createMockServerArgs(validLogsDir),
                List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
                terminal
            );
            assertThat(serverlessCli.onExitDiagnosticsAction, is(not(ServerlessServerCli.NO_OP_EXIT_ACTION)));
        }
    }

    public void testExecuteWithOnExitActionExits() throws Exception {
        var validTargetDir = createTempDir();
        var validLogsDir = createTempDir();
        var serverlessCli = newCommand();
        serverlessCli.initializeOnExitDiagnosticsAction(
            Map.of(ServerlessServerCli.DIAGNOSTICS_TARGET_PATH_SYSPROP, validTargetDir.toString()),
            createMockServerArgs(validLogsDir),
            List.of(ServerlessServerCli.HEAP_DUMP_PATH_JVM_OPT + "foo"),
            terminal
        );
        assertThat(serverlessCli.onExitDiagnosticsAction, is(not(ServerlessServerCli.NO_OP_EXIT_ACTION)));
        execute(serverlessCli);

        assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
        assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(0L));
    }

    public void testExecuteWithoutOnExitActionExits() throws Exception {
        sysprops.put(DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP, Long.toString(1));
        var serverlessCli = newCommand();
        serverlessCli.onExitDiagnosticsAction = ServerlessServerCli.NO_OP_EXIT_ACTION;
        execute(serverlessCli);

        assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
        assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(0L));
    }

    public void testExecuteWithExceptionOnExitActionExits() throws Exception {
        sysprops.put(DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP, Long.toString(1));
        var serverlessCli = newCommand();
        serverlessCli.onExitDiagnosticsAction = x -> { throw new RuntimeException("Bang"); };
        expectThrows(RuntimeException.class, () -> execute(serverlessCli));

        assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
        assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(0L));
    }

    public void testCallingCloseBeforeExecuteBlocks() throws Exception {
        sysprops.put(DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP, Long.toString(1));
        var serverlessCli = newCommand();
        serverlessCli.onExitDiagnosticsAction = x -> {
            assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
            assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(1L));
        };

        execute(serverlessCli);

        assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
        assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(0L));
    }

    public void testCallingCloseAfterExecuteFinishedDoNotBlock() throws Exception {
        sysprops.put(DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP, Long.toString(1));
        var serverlessCli = newCommand();
        serverlessCli.onExitDiagnosticsAction = ServerlessServerCli.NO_OP_EXIT_ACTION;

        execute(serverlessCli);

        assertThat(serverlessCli.serverlessCliFinishedLatch.get(), notNullValue());
        assertThat(serverlessCli.serverlessCliFinishedLatch.get().getCount(), is(0L));

        assertTimeout(Duration.ofSeconds(5), serverlessCli::close);
    }

    public void testCallingCloseBeforeExecuteFinishedBlocks() throws Exception {
        sysprops.put(DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP, Long.toString(1));
        var serverlessCli = newCommand();

        CountDownLatch exitActionStarted = new CountDownLatch(1);
        CountDownLatch finishExitAction = new CountDownLatch(1);

        serverlessCli.onExitDiagnosticsAction = x -> {
            exitActionStarted.countDown();
            try {
                finishExitAction.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        // Simulate the shutdown hook calling close after the exit action started
        var shutdownThread = new Thread(checked(() -> {
            exitActionStarted.await();
            serverlessCli.close();
        }));
        shutdownThread.start();

        var executeThread = new Thread(checked(() -> execute(serverlessCli)));
        executeThread.start();

        assertBusy(() -> assertSame(shutdownThread.getState(), Thread.State.WAITING));
        assertBusy(() -> assertSame(executeThread.getState(), Thread.State.WAITING));

        // Let the onExitDiagnosticsAction finish and exit
        finishExitAction.countDown();

        // Join the threads. A timeout here would indicate a problem. Both threads should exit "quickly", but let's give them some
        // larger margin to accommodate for slow CI machines.
        final int timeoutInMillis = 10000;
        // Once the onExitDiagnosticsAction is done, the execute action in the main thread should finish "quickly"
        executeThread.join(timeoutInMillis);
        // Once the execute action is finished, the close() action in the shutdown hook (simulated by shutdownThread) should also
        // finish "quickly"
        shutdownThread.join(timeoutInMillis);
    }

    public void testFastShutdownAlreadyExists() throws Exception {
        Path fastShutdownMarker = createTempFile();
        var serverlessCli = executeWithFastShutdown(fastShutdownMarker, null);

        // now shut it down. If force stop is not called we will block indefinitely in close and trip the timeout waiting on this thread
        var shutdownThread = new Thread(checked(serverlessCli::close));
        shutdownThread.start();

        shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
    }

    public void testFastShutdownCreatedDuringShutdown() throws Exception {
        Path fastShutdownMarker = createTempDir().resolve("shut-it-down");

        var stopRunningLatch = new CountDownLatch(1);
        var serverlessCli = executeWithFastShutdown(fastShutdownMarker, stopRunningLatch);

        // now shut it down. If force stop is not called we will block indefinitely in close and trip the timeout waiting on this thread
        var shutdownThread = new Thread(checked(serverlessCli::close));
        shutdownThread.start();

        // wait until we are actually in stop before creating the file
        assertTrue(stopRunningLatch.await(10, TimeUnit.SECONDS));
        Files.createFile(fastShutdownMarker);

        shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
    }

    private ServerlessServerCli executeWithFastShutdown(Path fastShutdownMarker, CountDownLatch stopRunningLatch) {
        sysprops.put(ServerlessServerCli.FAST_SHUTDOWN_MARKER_FILE_SYSPROP, fastShutdownMarker.toString());

        var executeBlockLatch = new CountDownLatch(1);
        var executeRunningLatch = new CountDownLatch(1);
        var stopLatch = new CountDownLatch(1);
        mockServer.waitForAction = () -> {
            executeRunningLatch.countDown();
            nonInterruptibleVoid(executeBlockLatch::await);
        };
        mockServer.stopAction = () -> {
            if (stopRunningLatch != null) {
                stopRunningLatch.countDown();
            }
            nonInterruptibleVoid(stopLatch::await);
        };
        mockServer.forceStopAction = () -> {
            executeBlockLatch.countDown();
            stopLatch.countDown();
        };
        var serverlessCli = newCommand();
        var executeThread = new Thread(checked(() -> execute(serverlessCli)));
        executeThread.start();

        // wait for the process to be "started"
        nonInterruptibleVoid(executeRunningLatch::await);

        return serverlessCli;
    }

    private static Settings emptySettings() {
        return Settings.builder().build();
    }

    private static ServerArgs createMockServerArgs(Path logsDir) {
        return new ServerArgs(false, false, null, KeyStoreWrapper.create(), null, null, logsDir);
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
        volatile Runnable waitForAction = null;
        volatile Runnable stopAction = null;
        volatile Runnable forceStopAction = null;

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
            if (waitForAction != null) {
                waitForAction.run();
            }
            return 0;
        }

        @Override
        public void stop() {
            if (stopAction != null) {
                stopAction.run();
            }
        }

        @Override
        public void forceStop() {
            if (forceStopAction != null) {
                forceStopAction.run();
            }
        }
    }

    @Override
    protected ServerlessServerCli newCommand() {
        return new ServerlessServerCli() {
            @Override
            protected Command loadTool(String toolname, String libs) {
                return new EnvironmentAwareCommand("NO-OP") {
                    @Override
                    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) {}
                };
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
            protected String getCgroupV2DirName() {
                return cgroupV2DirName;
            }

            @Override
            protected int getAvailableProcessors() {
                return availableProcessors;
            }
        };
    }

    private static void assertTimeout(Duration timeout, CheckedRunnable<Exception> supplier) throws ExecutionException,
        InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<Void> future = executorService.submit(() -> {
                try {
                    supplier.run();
                    return null;
                } catch (Throwable throwable) {
                    throw new AssertionError(throwable);
                }
            });

            long timeoutInMillis = timeout.toMillis();
            try {
                future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                throw new AssertionFailedError("Execution timed out");
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private static <E extends Exception> Runnable checked(CheckedRunnable<E> checkedRunnable) {
        return () -> {
            try {
                checkedRunnable.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
