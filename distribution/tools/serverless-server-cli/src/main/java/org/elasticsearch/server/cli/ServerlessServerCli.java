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

import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;

public class ServerlessServerCli extends ServerCli {

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
        Settings finalSettings = Settings.builder().put(defaultSettings).put(nodeSettings).build();
        var newEnv = new Environment(finalSettings, env.configFile());

        super.execute(terminal, options, newEnv, processInfo);
    }

    @Override
    protected SecureSettingsLoader secureSettingsLoader(Environment env) {
        return new LocallyMountedSecretsLoader();
    }
}
