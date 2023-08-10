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
import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterHandle;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.function.Predicate.not;

public class ServerlessLocalClusterHandle extends LocalClusterHandle {
    private static final Logger LOGGER = LogManager.getLogger(ServerlessLocalClusterHandle.class);

    private final String name;
    private final List<AbstractLocalClusterFactory.Node> nodes;
    private final Path baseWorkingDir;
    private final DistributionResolver distributionResolver;
    private final Lock nodeLock = new ReentrantLock();

    public ServerlessLocalClusterHandle(
        String name,
        Path baseWorkingDir,
        DistributionResolver distributionResolver,
        List<AbstractLocalClusterFactory.Node> nodes
    ) {
        super(name, nodes);
        this.name = name;
        this.baseWorkingDir = baseWorkingDir;
        this.distributionResolver = distributionResolver;
        this.nodes = nodes;
    }

    @Override
    public void stopNode(int index, boolean forcibly) {
        nodeLock.lock();
        // When stopping a node in serverless we want to actually replace it with a new node on subsequent startup since that is what is
        // most likely to happen in a kubernetes environment. Containers are unlikely to be restarted in-place, rather, if a pod is
        // terminated, a new one will be created to replace it.
        AbstractLocalClusterFactory.Node oldNode = nodes.get(index);
        AbstractLocalClusterFactory.Node newNode = new AbstractLocalClusterFactory.Node(
            baseWorkingDir,
            distributionResolver,
            oldNode.getSpec(),
            RandomStringUtils.randomAlphabetic(7)
        );

        nodes.set(index, newNode);
        nodeLock.unlock();

        LOGGER.info("Stopping node '{}'. It will be replaced by node '{}' on subsequent startup.", oldNode.getName(), newNode.getName());
        oldNode.stop(forcibly);

    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        nodeLock.lock();
        AbstractLocalClusterFactory.Node oldNode = nodes.get(index);
        AbstractLocalClusterFactory.Node newNode = new AbstractLocalClusterFactory.Node(
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
        nodeLock.unlock();
    }

    @Override
    public void upgradeToVersion(Version version) {
        LOGGER.info("Upgrading serverless Elasticsearch cluster '{}'", name);

        List<AbstractLocalClusterFactory.Node> searchNodes = nodes.stream().filter(n -> n.getSpec().hasRole("search")).toList();
        List<AbstractLocalClusterFactory.Node> otherNodes = nodes.stream().filter(not(n -> n.getSpec().hasRole("search"))).toList();
        searchNodes.forEach(n -> upgradeNodeToVersion(n, version));
        otherNodes.forEach(n -> upgradeNodeToVersion(n, version));
    }

    private void upgradeNodeToVersion(AbstractLocalClusterFactory.Node node, Version version) {
        upgradeNodeToVersion(nodes.indexOf(node), version);
    }
}
