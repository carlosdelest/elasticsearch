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
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ServerlessServerCli extends ServerCli {

    static final String PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP = "es.serverless.processors_overcommit_factor";
    static final String FAST_SHUTDOWN_MARKER_FILE_SYSPROP = "es.serverless.fast_shutdown_marker_file";
    static final String APM_PROJECT_ID_SETTING = "telemetry.agent.global_labels.project_id";
    static final String APM_PROJECT_TYPE_SETTING = "telemetry.agent.global_labels.project_type";
    static final String DIAGNOSTICS_TARGET_PATH_SYSPROP = "es.serverless.path.diagnostics";
    private static final String DEFAULT_DIAGNOSTICS_TARGET_PATH = "/mnt/elastic/diagnostics";
    static final String DIAGNOSTICS_ACTION_TIMEOUT_SECONDS_SYSPROP = "es.serverless.diagnostics_action.timeout";

    static final String HEAP_DUMP_PATH_JVM_OPT = "-XX:HeapDumpPath=";
    static final String REPLAY_DATA_FILE_JVM_OPT = "-XX:ReplayDataFile=";
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

    void moveDiagnosticsToTargetPath(
        int exitCode,
        Path targetPath,
        Path heapDumpDataDir,
        Path replayDir,
        Path logsDir,
        Settings settings,
        Terminal terminal
    ) throws UserException {
        try {
            // We do not look for a specific exitCode. The exit code for a OOME error should be 3 (https://ela.st/oome-exit-code) but it is
            // undocumented and so it seems better not to rely on it. Instead, we just look for a non-normal exit and for the presence
            // of a dump file
            if (exitCode == ExitCodes.OK) {
                terminal.println("Process terminated with 0 exit code, skipping post-exit diagnostic collection");
                return;
            }
            var dumpFiles = FileSystemUtils.files(heapDumpDataDir, "*.hprof*");
            if (dumpFiles.length == 0) {
                terminal.println("No hprof files found, skipping post-exit diagnostic collection");
                return;
            }

            Path[] replayFiles = new Path[0];
            if (replayDir.equals(logsDir) == false) {
                // Replay files will be already included in logs
                if (Files.isDirectory(replayDir) == false) {
                    terminal.errorPrintln(
                        Strings.format("%s is not a valid directory. The diagnostic file will not contain replay files.", replayDir)
                    );
                } else {
                    replayFiles = FileSystemUtils.files(replayDir, "replay_*.log");
                }
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
                for (Path dumpFile : dumpFiles) {
                    String target = heapDumpDataDir.relativize(dumpFile).toString();
                    stream.putNextEntry(new ZipEntry(target));
                    Files.copy(dumpFile, stream);
                }

                for (Path replayFile : replayFiles) {
                    String target = replayDir.relativize(replayFile).toString();
                    stream.putNextEntry(new ZipEntry(target));
                    Files.copy(replayFile, stream);
                }

                if (Files.isDirectory(logsDir) == false) {
                    terminal.errorPrintln(
                        Strings.format("%s is not a valid directory. The diagnostic file will not contain log files.", logsDir)
                    );
                } else {
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
            terminal.println("Cleaning up dump files: " + Stream.of(dumpFiles).map(Path::toString).collect(Collectors.joining(";")));
            IOUtils.deleteFilesIgnoringExceptions(dumpFiles);
            if (replayFiles.length > 0) {
                terminal.println(
                    "Cleaning up replay files: " + Stream.of(replayFiles).map(Path::toString).collect(Collectors.joining(";"))
                );
                IOUtils.deleteFilesIgnoringExceptions(replayFiles);
            }
            terminal.println("Finished cleaning up");
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Cannot copy diagnostic information to {targetPath}", e);
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

    void initializeOnExitDiagnosticsAction(Map<String, String> sysProps, ServerArgs args, List<String> jvmOptions, Terminal terminal) {
        final String diagnosticsDir = sysProps.getOrDefault(DIAGNOSTICS_TARGET_PATH_SYSPROP, DEFAULT_DIAGNOSTICS_TARGET_PATH);

        var heapDumpDataDir = extractJvmOptionValue(HEAP_DUMP_PATH_JVM_OPT, jvmOptions);
        var replayFile = extractJvmOptionValue(REPLAY_DATA_FILE_JVM_OPT, jvmOptions);
        var replayDir = replayFile.map(p -> Paths.get(p).getParent()).orElse(args.logsDir());
        if (diagnosticsDir == null || Files.isDirectory(Paths.get(diagnosticsDir)) == false) {
            terminal.errorPrintln(
                Strings.format("The diagnostic target path [%s] is not correctly set; OOME diagnostic will not be saved.", diagnosticsDir)
            );
        } else if (heapDumpDataDir.isEmpty()) {
            terminal.errorPrintln("The heap dump path is not correctly set; OOME diagnostic will not be saved.");
        } else {
            var diagnosticsPath = Paths.get(diagnosticsDir);
            var heapDumpDataPath = Paths.get(heapDumpDataDir.get());
            terminal.println(
                Strings.format(
                    "OOME dumps will be read from [%s] and saved to the diagnostic target path [%s].",
                    heapDumpDataPath.toAbsolutePath().toString(),
                    diagnosticsPath.toAbsolutePath().toString()
                )
            );
            onExitDiagnosticsAction = exitCode -> {
                terminal.println("Starting post-exit diagnostic collection, exit code is " + exitCode);
                moveDiagnosticsToTargetPath(
                    exitCode,
                    diagnosticsPath,
                    heapDumpDataPath,
                    replayDir,
                    args.logsDir(),
                    args.nodeSettings(),
                    terminal
                );
            };
        }
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

        Path sharesFile = getCgroupFs().resolve("cpu/cpu.shares");
        if (Files.exists(sharesFile) == false) {
            throw new IllegalStateException("cgroups v1 cpu.shares must be set in serverless");
        }

        int shares = Integer.parseInt(Files.readString(sharesFile).strip());
        double vcpus = shares / 1024.0;
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

        builder.put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), allocated);
    }

    protected Path getCgroupFs() {
        return Paths.get("/sys/fs/cgroup");
    }

    protected int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

}
