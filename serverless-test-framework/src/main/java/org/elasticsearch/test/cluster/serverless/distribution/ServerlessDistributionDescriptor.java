/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless.distribution;

import org.elasticsearch.test.cluster.local.distribution.DefaultDistributionDescriptor;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;

public class ServerlessDistributionDescriptor extends DefaultDistributionDescriptor {
    private final Path extractedDir;

    public ServerlessDistributionDescriptor(Version version, boolean snapshot, Path extractedDir, DistributionType type) {
        super(Version.CURRENT, snapshot, extractedDir, type);
        this.extractedDir = extractedDir;
    }

    @Override
    public Path getDistributionDir() {
        return extractedDir;
    }

    @Override
    public String toString() {
        return "ServerlessDistributionDescriptor{"
            + "distributionDir="
            + getDistributionDir()
            + ", version="
            + getVersion()
            + ", snapshot="
            + isSnapshot()
            + ", type="
            + getType()
            + '}';
    }
}
