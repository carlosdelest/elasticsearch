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
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;

public class ServerlessDistributionDescriptor implements DistributionDescriptor {
    private final DistributionDescriptor delegate;

    public ServerlessDistributionDescriptor(DistributionDescriptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Version getVersion() {
        return delegate.getVersion();
    }

    @Override
    public boolean isSnapshot() {
        return delegate.isSnapshot();
    }

    @Override
    public Path getDistributionDir() {
        // Serverless distributions don't include a version in the path, so we hard-code this to "elasticsearch"
        return delegate.getDistributionDir().getParent().resolve("elasticsearch");
    }

    @Override
    public DistributionType getType() {
        return delegate.getType();
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
