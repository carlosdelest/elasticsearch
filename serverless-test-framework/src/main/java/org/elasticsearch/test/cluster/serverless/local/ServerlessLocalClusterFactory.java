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

package org.elasticsearch.test.cluster.serverless.local;

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
                .map(s -> new Node(baseWorkingDir, distributionResolver, s, RandomStringUtils.randomAlphabetic(7), true))
                .sorted((a, b) -> {
                    // Sort search nodes first to ease the upgrade process (search nodes must be upgraded first)
                    boolean isSearchA = a.getSpec().hasRole("search");
                    boolean isSearchB = b.getSpec().hasRole("search");
                    return Boolean.compare(isSearchB, isSearchA);
                })
                .collect(Collectors.toList())
        );
    }
}
