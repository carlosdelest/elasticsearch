/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;

import java.nio.file.Path;
import java.util.stream.Collectors;

public class ServerlessLocalClusterFactory extends AbstractLocalClusterFactory<LocalClusterSpec, ServerlessLocalClusterHandle> {
    private final DistributionResolver distributionResolver;

    public ServerlessLocalClusterFactory(DistributionResolver distributionResolver) {
        super(distributionResolver);
        this.distributionResolver = distributionResolver;
    }

    @Override
    protected ServerlessLocalClusterHandle createHandle(Path baseWorkingDir, LocalClusterSpec spec) {
        return new ServerlessLocalClusterHandle(
            spec.getName(),
            baseWorkingDir,
            distributionResolver,
            spec.getNodes()
                .stream()
                .map(s -> new Node(baseWorkingDir, distributionResolver, s, RandomStringUtils.randomAlphabetic(7)))
                .collect(Collectors.toList())
        );
    }
}
