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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
