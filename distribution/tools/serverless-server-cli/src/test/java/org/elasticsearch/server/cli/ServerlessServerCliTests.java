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

import joptsimple.OptionSet;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessServerCliTests extends CommandTestCase {

    private final MockServerlessProcess mockServer = new MockServerlessProcess();

    @Before
    public void setupMockConfig() throws IOException {
        Files.createFile(configDir.resolve("log4j2.properties"));
    }

    public void testDefaultsOverridden() throws Exception {
        Path defaultSettingsFile = configDir.resolve("serverless-default-settings.yml");
        Files.writeString(defaultSettingsFile, """
            foo.bar: a-default
            """);
        execute("-E", "foo.bar=override");
        assertThat(mockServer.args.nodeSettings().get("foo.bar"), equalTo("override"));
        assertThat(terminal.getOutput(), containsString("Serverless default for [foo.bar] is overridden to [override]"));
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
        };
    }
}
