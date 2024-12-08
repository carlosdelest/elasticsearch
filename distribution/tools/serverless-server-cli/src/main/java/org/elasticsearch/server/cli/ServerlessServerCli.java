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

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import joptsimple.OptionSet;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeRoleSettings;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ServerlessServerCli extends ServerCli {

    static final String PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP = "es.serverless.processors_overcommit_factor";
    static final String FAST_SHUTDOWN_MARKER_FILE_SYSPROP = "es.serverless.fast_shutdown_marker_file";
    static final String FILE_LOGGING_SYSPROP = "es.serverless.file_logging";
    static final String APM_PROJECT_ID_SETTING = "telemetry.agent.global_labels.project_id";
    static final String APM_NODE_ROLE_SETTING = "telemetry.agent.global_labels.node_tier";
    static final String APM_PROJECT_TYPE_SETTING = "telemetry.agent.global_labels.project_type";
    static final String DIAGNOSTICS_TARGET_PATH_SYSPROP = "es.serverless.path.diagnostics";
    private static final String DEFAULT_DIAGNOSTICS_TARGET_PATH = "/mnt/elastic/diagnostics";
    static final String DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP = "es.serverless.diagnostics_action.timeout";

    static final String HEAP_DUMP_PATH_JVM_OPT = "-XX:HeapDumpPath=";
    static final String REPLAY_DATA_FILE_JVM_OPT = "-XX:ReplayDataFile=";
    static final String FATAL_ERROR_LOG_FILE_JVM_OPT = "-XX:ErrorFile=";
    private static final DateTimeFormatter ZIP_FILE_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
        .withZone(ZoneId.from(ZoneOffset.UTC));

    static final CheckedConsumer<Integer, UserException> NO_OP_EXIT_ACTION = exitCode -> {};
    CheckedConsumer<Integer, UserException> onExitDiagnosticsAction = NO_OP_EXIT_ACTION;
    volatile FastShutdownFileWatcher fastShutdownWatcher = null;

    final AtomicReference<CountDownLatch> serverlessCliFinishedLatch = new AtomicReference<>();

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        terminal.println("Starting Serverless Elasticsearch...");

        if (processInfo.sysprops().containsKey(FAST_SHUTDOWN_MARKER_FILE_SYSPROP)) {
            var file = processInfo.sysprops().get(FAST_SHUTDOWN_MARKER_FILE_SYSPROP);
            fastShutdownWatcher = new FastShutdownFileWatcher(terminal, Paths.get(file), this::getServer);
        }

        String fileLogging = processInfo.sysprops().get(FILE_LOGGING_SYSPROP);
        if (fileLogging != null && Booleans.parseBoolean(fileLogging)) {
            Path loggingConfig = env.configFile().resolve("log4j2.properties");
            Path serverlessLoggingConfig = env.configFile().resolve("log4j2.serverless.properties");
            // overwrite with new file based logging config
            Files.copy(serverlessLoggingConfig, loggingConfig, StandardCopyOption.REPLACE_EXISTING);
        }

        try {

            Path defaultsFile = env.configFile().resolve("serverless-default-settings.yml");
            if (Files.exists(defaultsFile) == false) {
                throw new IllegalStateException("Missing serverless defaults");
            }

            Settings defaultSettings = Settings.builder().loadFromPath(defaultsFile).build();
            Settings nodeSettings = env.settings();

            for (String defaultSettingName : defaultSettings.keySet()) {
                if (nodeSettings.hasValue(defaultSettingName)) {
                    String overrideValue = nodeSettings.get(defaultSettingName);
                    terminal.println("Serverless default for [%1s] is overridden to [%1s]".formatted(defaultSettingName, overrideValue));
                }
            }
            Settings.Builder finalSettingsBuilder = Settings.builder().put(defaultSettings).put(nodeSettings);
            boolean isLinux = processInfo.sysprops().get("os.name").startsWith("Linux");
            if (isLinux && processInfo.sysprops().containsKey(PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP)) {
                // cgroups only apply to production on linux, no local development
                double overcommit = Double.parseDouble(processInfo.sysprops().get(PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP));
                addNodeProcessors(finalSettingsBuilder, overcommit, terminal);
            }

            // copy project id to apm attributes, but be lenient for tests that don't set project id...
            if (ServerlessSharedSettings.PROJECT_ID.exists(nodeSettings)) {
                finalSettingsBuilder.put(APM_PROJECT_ID_SETTING, ServerlessSharedSettings.PROJECT_ID.get(nodeSettings));
            }
            if (ServerlessSharedSettings.PROJECT_TYPE.exists(nodeSettings)) {
                finalSettingsBuilder.put(APM_PROJECT_TYPE_SETTING, ServerlessSharedSettings.PROJECT_TYPE.get(nodeSettings));
            }
            if (NodeRoleSettings.NODE_ROLES_SETTING.exists(nodeSettings)) {
                finalSettingsBuilder.put(APM_NODE_ROLE_SETTING, indexOrSearchOrMlNodeRole(nodeSettings));
            }

            var newEnv = new Environment(finalSettingsBuilder.build(), env.configFile());

            serverlessCliFinishedLatch.set(new CountDownLatch(1));

            super.execute(terminal, options, newEnv, processInfo);
        } finally {
            var latch = serverlessCliFinishedLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    /**
     * This is used for setting  {@link ServerlessServerCli#APM_NODE_ROLE_SETTING}
     * which will send as a field in metrics and stored as a keyword field.
     * In production deployment we are expecting only one role per node (either index, search or ml), however in local development env
     * you can start nodes with multiple values.
     * Also, node has additional node roles like master, remote etc which we do not care about on this metric field
     *
     * This method picks either index, search or ml role (whichever first) or defaults to ''
     */
    private static String indexOrSearchOrMlNodeRole(Settings nodeSettings) {
        Set<DiscoveryNodeRole> nodeRoles = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.ML_ROLE);

        return NodeRoleSettings.NODE_ROLES_SETTING.get(nodeSettings)
            .stream()
            .filter(nodeRoles::contains)
            .map(DiscoveryNodeRole::roleName)
            .findFirst()
            .orElse("");
    }

    @Override
    public void close() throws IOException {
        if (fastShutdownWatcher != null) {
            fastShutdownWatcher.start();
        }
        super.close();
        try {
            var latch = serverlessCliFinishedLatch.get();
            if (latch != null) {
                latch.await();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("Thread got interrupted while waiting for the serverless CLI process to shutdown.");
        }
        if (fastShutdownWatcher != null) {
            // stop this watcher to kill its own thread. it doesn't matter in the real world, but without it tests have thread leaks.
            // ideally there would be a "watch for the file one time" option so it would self-stop
            fastShutdownWatcher.stop();
        }
    }

    private static Path[] safeFileSystemUtilsFiles(Path dir, String glob, Consumer<IOException> onFailure) {
        try {
            if (Files.isDirectory(dir)) {
                return FileSystemUtils.files(dir, glob);
            }
        } catch (IOException ex) {
            onFailure.accept(ex);
        }
        return new Path[0];
    }

    private static void addFilesToZip(Path filesSourceDir, Path[] files, ZipOutputStream stream) throws IOException {
        for (var file : files) {
            String target = filesSourceDir.relativize(file).toString();
            stream.putNextEntry(new ZipEntry(target));
            Files.copy(file, stream);
        }
    }

    void moveDiagnosticsToTargetPath(
        int exitCode,
        Path targetPath,
        Path heapDumpDataDir,
        Path logsDir,
        Settings settings,
        Terminal terminal
    ) throws UserException {
        try {
            // We do not look for a specific exitCode: there might be different exit codes for different errors/crashes, and they might be
            // not documented (e.g. the exit code for a OOME error should be 3 (https://ela.st/oome-exit-code) but it is not officially
            // documented).
            // Instead, we just look for a non-normal exit and for the presence of one or more diagnostic files we care about.
            if (exitCode == ExitCodes.OK) {
                terminal.println("Process terminated with 0 exit code, skipping post-exit diagnostic collection");
                return;
            }

            var fatalErrorLogFiles = safeFileSystemUtilsFiles(
                logsDir,
                "hs_err_*.log",
                ex -> terminal.errorPrintln("Error listing fatal error logs: " + ex)
            );
            var heapDumpFiles = safeFileSystemUtilsFiles(
                heapDumpDataDir,
                "*.hprof*",
                ex -> terminal.errorPrintln("Error listing heap dump files: " + ex)
            );

            if (heapDumpFiles.length == 0 && fatalErrorLogFiles.length == 0) {
                terminal.println("No diagnostic files found, skipping post-exit diagnostic collection");
                return;
            }

            String zipFileName = "";
            if (ServerlessSharedSettings.PROJECT_ID.exists(settings)) {
                zipFileName += ServerlessSharedSettings.PROJECT_ID.get(settings) + "_";
            }
            final String nodeName = settings.get("node.name");
            if (nodeName != null) {
                zipFileName += nodeName + "_";
            }
            zipFileName += ZIP_FILE_SUFFIX_FORMATTER.format(Instant.now());

            Path zip = targetPath.resolve(zipFileName + ".zip");

            MessageDigest md = MessageDigests.sha256();
            try (ZipOutputStream stream = new ZipOutputStream(new DigestOutputStream(Files.newOutputStream(zip), md))) {
                addFilesToZip(heapDumpDataDir, heapDumpFiles, stream);

                {
                    Files.walkFileTree(logsDir, new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            String target = "logs/" + logsDir.relativize(file);
                            stream.putNextEntry(new ZipEntry(target));
                            Files.copy(file, stream);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
            }
            terminal.println(
                Strings.format(
                    "Diagnostic information saved to [%s]; SHA256: [%s]; Size: [%d]",
                    zip.toAbsolutePath(),
                    MessageDigests.toHexString(md.digest()),
                    Files.size(zip)
                )
            );

            // Clean up files inserted in the diagnostic bundle, to avoid duplication and conflict over names (which may lead to "stale"
            // dump files). This is particularly relevant in a Docker environment, where the PID is often stable (always the same, across
            // container restarts).
            if (heapDumpFiles.length > 0) {
                terminal.println(
                    "Cleaning up dump files: " + Stream.of(heapDumpFiles).map(Path::toString).collect(Collectors.joining(";"))
                );
                IOUtils.deleteFilesIgnoringExceptions(heapDumpFiles);
            }
            if (fatalErrorLogFiles.length > 0) {
                terminal.println(
                    "Cleaning up fatal error logs: " + Stream.of(fatalErrorLogFiles).map(Path::toString).collect(Collectors.joining(";"))
                );
                IOUtils.deleteFilesIgnoringExceptions(fatalErrorLogFiles);
            }
            terminal.println("Finished cleaning up");
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Cannot save diagnostic information", e);
        }
    }

    @Override
    protected synchronized ServerProcess startServer(Terminal terminal, ProcessInfo processInfo, ServerArgs args) throws Exception {
        var tempDir = ServerProcessUtils.setupTempDir(processInfo);
        var jvmOptions = JvmOptionsParser.determineJvmOptions(args, processInfo, tempDir, new ServerlessMachineDependentHeap());

        initializeOnExitDiagnosticsAction(processInfo.sysprops(), args, jvmOptions, terminal);

        var serverProcessBuilder = new ServerProcessBuilder().withTerminal(terminal)
            .withProcessInfo(processInfo)
            .withServerArgs(args)
            .withTempDir(tempDir)
            .withJvmOptions(jvmOptions);
        return serverProcessBuilder.start();
    }

    private static Optional<String> extractJvmOptionValue(String optionPrefix, List<String> jvmOptions) {
        return jvmOptions.stream().filter(x -> x.startsWith(optionPrefix)).findFirst().map(x -> x.substring(optionPrefix.length()));
    }

    private static boolean isPathInLogs(String fileOrDirectoryName, Path logsDir) throws IOException {
        var path = Paths.get(fileOrDirectoryName);
        var dir = Files.isDirectory(path) ? path : path.getParent();
        return Files.isSameFile(dir, logsDir);
    }

    void initializeOnExitDiagnosticsAction(Map<String, String> sysProps, ServerArgs args, List<String> jvmOptions, Terminal terminal) {
        final String diagnosticsDir = sysProps.getOrDefault(DIAGNOSTICS_TARGET_PATH_SYSPROP, DEFAULT_DIAGNOSTICS_TARGET_PATH);
        var heapDumpDataDir = extractJvmOptionValue(HEAP_DUMP_PATH_JVM_OPT, jvmOptions);

        if (diagnosticsDir == null || Files.isDirectory(Paths.get(diagnosticsDir)) == false) {
            terminal.errorPrintln(
                Strings.format("The diagnostic target path [%s] is not correctly set; error diagnostic will not be saved.", diagnosticsDir)
            );
            return;
        }
        if (heapDumpDataDir.isEmpty()) {
            terminal.errorPrintln("The heap dump path is not correctly set; error diagnostic will not be saved.");
            return;
        }
        if (args.logsDir() == null || Files.isDirectory(args.logsDir()) == false) {
            terminal.errorPrintln(
                Strings.format(
                    "The logs path [%s] is not a valid directory. The diagnostic bundle will not contain log files.",
                    args.logsDir()
                )
            );
            return;
        }

        var replayFile = extractJvmOptionValue(REPLAY_DATA_FILE_JVM_OPT, jvmOptions);
        var fatalErrorLogFile = extractJvmOptionValue(FATAL_ERROR_LOG_FILE_JVM_OPT, jvmOptions);

        if (replayFile.isPresent()) {
            try {
                if (isPathInLogs(replayFile.get(), args.logsDir()) == false) {
                    terminal.errorPrintln(
                        Strings.format(
                            "The replay file path [%s] is not set to the logs path [%s]; "
                                + "replay files will not be added to the diagnostic bundle.",
                            replayFile.get(),
                            args.logsDir()
                        )
                    );
                }
            } catch (IOException ex) {
                terminal.errorPrintln(
                    Strings.format(
                        "Cannot check the replay file path [%s]; replay files will not be added to the diagnostic bundle: [%s]",
                        replayFile.get(),
                        ex
                    )
                );
            }
        }
        if (fatalErrorLogFile.isPresent()) {
            try {
                if (isPathInLogs(fatalErrorLogFile.get(), args.logsDir()) == false) {
                    terminal.errorPrintln(
                        Strings.format(
                            "The fatal error log path [%s] is not set to the logs path [%s]; "
                                + "fatal error logs will not be added to the diagnostic bundle.",
                            fatalErrorLogFile.get(),
                            args.logsDir()
                        )
                    );
                }
            } catch (IOException ex) {
                terminal.errorPrintln(
                    Strings.format(
                        "Cannot check fatal error log path [%s]; fatal error logs will not be added to the diagnostic bundle: [%s]",
                        fatalErrorLogFile.get(),
                        ex
                    )
                );
            }
        }

        var diagnosticsPath = Paths.get(diagnosticsDir);
        var heapDumpDataPath = Paths.get(heapDumpDataDir.get());
        terminal.println(
            Strings.format(
                "Heap dumps will be read from [%s] and saved to the diagnostic target path [%s].",
                heapDumpDataPath.toAbsolutePath().toString(),
                diagnosticsPath.toAbsolutePath().toString()
            )
        );
        onExitDiagnosticsAction = exitCode -> {
            terminal.println("Starting post-exit diagnostic collection, exit code is " + exitCode);
            moveDiagnosticsToTargetPath(exitCode, diagnosticsPath, heapDumpDataPath, args.logsDir(), args.nodeSettings(), terminal);
        };
    }

    @Override
    protected void onExit(int exitCode) throws UserException {
        onExitDiagnosticsAction.accept(exitCode);
        super.onExit(exitCode);
    }

    @Override
    protected SecureSettingsLoader secureSettingsLoader(Environment env) {
        return new LocallyMountedSecretsLoader();
    }

    /**
     * Adds node.processors based on k8s cpu.request and overcommit factor.
     */
    void addNodeProcessors(Settings.Builder builder, double overcommit, Terminal terminal) throws IOException {
        if (EsExecutors.NODE_PROCESSORS_SETTING.exists(builder)) {
            throw new IllegalStateException("node.processors must not be present, it will be auto calculated");
        }
        if (ServerlessSharedSettings.VCPU_REQUEST.exists(builder)) {
            throw new IllegalStateException("node.vcpu_request must not be present, it will be auto calculated");
        }

        double vcpus;
        if (Files.exists(getCgroupFs().resolve("cgroup.controllers"))) {
            // gcoups v2
            Path weightFile = getCgroupFs().resolve(getCgroupV2DirName()).resolve("cpu.weight");
            if (Files.exists(weightFile)) {
                int weight = Integer.parseInt(Files.readString(weightFile).strip());
                vcpus = weight / 100.0;
            } else {
                throw new IllegalStateException("In cgroups v2, cpu.weight must be set in serverless");
            }
        } else {
            // gcoups v1
            Path sharesFile = getCgroupFs().resolve("cpu/cpu.shares");
            if (Files.exists(sharesFile)) {
                int shares = Integer.parseInt(Files.readString(sharesFile).strip());
                vcpus = shares / 1024.0;
            } else {
                throw new IllegalStateException("In cgroups v1, cpu.shares must be set in serverless");
            }
        }

        double allocated = vcpus * overcommit;

        int available = getAvailableProcessors();
        if (allocated > available) {
            terminal.println(
                String.format(
                    Locale.ROOT,
                    "Capping cpu overcommit to (%d). vCPUs (%f) * overcommit (%f) results in node.processors (%f)"
                        + " greater than available cpus (%d)",
                    available,
                    vcpus,
                    overcommit,
                    allocated,
                    available
                )
            );
            allocated = available;
        }

        builder.put(ServerlessSharedSettings.VCPU_REQUEST.getKey(), vcpus);
        builder.put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), allocated);
    }

    protected Path getCgroupFs() {
        return Paths.get("/sys/fs/cgroup");
    }

    /**
     * Call this on v2 only. Can't cope with multiple cgroup hierarchies.
     */
    protected String getCgroupV2DirName() throws IOException {
        var lines = Files.readAllLines(Paths.get("/proc/self/cgroup"));
        if (lines.size() != 1) {
            throw new IllegalStateException("v2 cgroups must contain only one line");
        }
        var fullPath = lines.getFirst().split(":")[2];

        // The path is relative but has a leading slash for some reason.
        // Isolate such weirdness to this method so the rest of the logic makes sense.
        assert fullPath.startsWith("/");
        return fullPath.substring(1);
    }

    protected int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

}
