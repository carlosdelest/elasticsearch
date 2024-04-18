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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;

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
import java.util.Objects;
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

        final String type = properties.getProperty("type");
        final String client = properties.getProperty("client");

        if ("s3".equals(type) == false) {
            // this codebase only supports s3, support for other object storage types will need to be added as
            // future enhancements
            throw new IllegalArgumentException("'type' was [" + type + "], but only 's3' is supported");
        }

        if ("default".equals(client) == false) {
            System.err.println("warning: 'client' was [" + client + "], but 'default' is expected");
        }

        final String endpoint = Objects.requireNonNullElse(properties.getProperty("endpoint"), Constants.S3_HOSTNAME);
        final String accessKey = Objects.requireNonNull(properties.getProperty("access_key"));
        final String secretKey = Objects.requireNonNull(properties.getProperty("secret_key"));

        final String bucket = Objects.requireNonNull(properties.getProperty("bucket"));
        final String basePath = Objects.requireNonNull(properties.getProperty("base_path"));

        AmazonS3 s3Client = S3ClientHelper.buildClient(endpoint, accessKey, secretKey);
        WipeDataOperation operation = new WipeDataOperation(s3Client, bucket, basePath, () -> System.out.print("."));
        operation.deleteBlobs();
        System.out.println(".");
    }

}
