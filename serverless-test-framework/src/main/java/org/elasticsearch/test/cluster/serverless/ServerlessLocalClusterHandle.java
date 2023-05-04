/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.local.LocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterHandle;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;
import java.util.List;

import static java.util.function.Predicate.not;

public class ServerlessLocalClusterHandle extends LocalClusterHandle {
    private static final Logger LOGGER = LogManager.getLogger(ServerlessLocalClusterHandle.class);

    private final String name;
    private final List<LocalClusterFactory.Node> nodes;
    private final Path baseWorkingDir;
    private final DistributionResolver distributionResolver;

    public ServerlessLocalClusterHandle(
        String name,
        Path baseWorkingDir,
        DistributionResolver distributionResolver,
        List<LocalClusterFactory.Node> nodes
    ) {
        super(name, nodes);
        this.name = name;
        this.baseWorkingDir = baseWorkingDir;
        this.distributionResolver = distributionResolver;
        this.nodes = nodes;
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        synchronized (nodes) {
            LocalClusterFactory.Node oldNode = nodes.get(index);
            LocalClusterFactory.Node newNode = new LocalClusterFactory.Node(
                baseWorkingDir,
                distributionResolver,
                oldNode.getSpec(),
                RandomStringUtils.randomAlphabetic(7)
            );

            LOGGER.info("Replacing node '{}' with node '{}'", oldNode.getName(), newNode.getName());

            nodes.add(index, newNode);
            newNode.start(version);
            waitUntilReady();
            oldNode.stop(false);
            nodes.remove(oldNode);
            waitUntilReady();
        }
    }

    @Override
    public void upgradeToVersion(Version version) {
        LOGGER.info("Upgrading serverless Elasticsearch cluster '{}'", name);

        List<LocalClusterFactory.Node> searchNodes = nodes.stream().filter(n -> n.getSpec().hasRole("search")).toList();
        List<LocalClusterFactory.Node> otherNodes = nodes.stream().filter(not(n -> n.getSpec().hasRole("search"))).toList();
        searchNodes.forEach(n -> upgradeNodeToVersion(n, version));
        otherNodes.forEach(n -> upgradeNodeToVersion(n, version));
    }

    private void upgradeNodeToVersion(LocalClusterFactory.Node node, Version version) {
        upgradeNodeToVersion(nodes.indexOf(node), version);
    }
}
