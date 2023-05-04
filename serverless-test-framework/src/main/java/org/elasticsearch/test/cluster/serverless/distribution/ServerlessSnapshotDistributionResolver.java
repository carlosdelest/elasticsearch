/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless.distribution;

import org.elasticsearch.test.cluster.local.distribution.DistributionDescriptor;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Files;
import java.nio.file.Path;

public class ServerlessSnapshotDistributionResolver extends ReleasedDistributionResolver {
    private static final String BWC_DISTRIBUTION_SYSPROP_PREFIX = "tests.release.distribution.";

    @Override
    public DistributionDescriptor resolve(Version version, DistributionType type) {
        String distributionPath = System.getProperty(BWC_DISTRIBUTION_SYSPROP_PREFIX + version.toString());

        if (distributionPath == null) {
            String taskPath = System.getProperty("tests.task");
            String project = taskPath.substring(0, taskPath.lastIndexOf(':'));
            String taskName = taskPath.substring(taskPath.lastIndexOf(':') + 1);

            throw new IllegalStateException(
                "Cannot locate Elasticsearch distribution. Ensure you've added the following to the build script for project '"
                    + project
                    + "':\n\n"
                    + "task {\n"
                    + "  "
                    + taskName
                    + " {\n"
                    + "    usesBwcDistribution()\n"
                    + "  }\n"
                    + "}"
            );
        }

        Path distributionDir = Path.of(distributionPath);
        if (Files.notExists(distributionDir)) {
            throw new IllegalStateException(
                "Cannot locate Elasticsearch distribution. Directory at '" + distributionDir + "' does not exist."
            );
        }

        return new ServerlessDistributionDescriptor(version, true, distributionDir, DistributionType.DEFAULT);
    }
}
