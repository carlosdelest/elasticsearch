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

package org.elasticsearch.wipe.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class ServerlessWipeDataCli extends Command {
    private final OptionSpec<String> configurationPath;

    public ServerlessWipeDataCli() {
        super("Wipe the data directory (on object storage) for a serverless project");
        configurationPath = parser.acceptsAll(List.of("c", "configuration"), "Configuration (properties) file")
            .withRequiredArg()
            .required();
    }

    @SuppressForbidden(reason = "file arg for cli")
    private static Path getPath(String file) {
        return PathUtils.get(file);
    }

    public Properties getProperties(OptionSet options) throws IOException {
        Path credentials = getPath(options.valueOf(configurationPath));
        Properties properties = new Properties();
        try (InputStream is = Files.newInputStream(credentials)) {
            properties.load(is);
        }
        return properties;
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        final Properties properties = getProperties(options);
        WipeDataOperation operation = WipeDataOperation.create(properties);

        operation.deleteBlobs();
        System.out.println(".");
    }

}
