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

import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public class ServerlessServerCli extends ServerCli {

    static final String PROCESSORS_OVERCOMMIT_FACTOR_SYSPROP = "es.serverless.processors_overcommit_factor";
    static final String APM_PROJECT_ID_SETTING = "telemetry.agent.global_labels.project_id";

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        terminal.println("Starting Serverless Elasticsearch...");

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

        var newEnv = new Environment(finalSettingsBuilder.build(), env.configFile());

        super.execute(terminal, options, newEnv, processInfo);
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
