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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory.Node;
import org.elasticsearch.test.cluster.local.DefaultLocalClusterHandle;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.function.Predicate.not;

public class ServerlessLocalClusterHandle extends DefaultLocalClusterHandle {
    private static final Logger LOGGER = LogManager.getLogger(ServerlessLocalClusterHandle.class);

    private final String name;
    private final List<Node> nodes;
    private final Path baseWorkingDir;
    private final DistributionResolver distributionResolver;
    private final Lock nodeLock = new ReentrantLock();

    public ServerlessLocalClusterHandle(String name, Path baseWorkingDir, DistributionResolver distributionResolver, List<Node> nodes) {
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
        Node oldNode = nodes.get(index);
        Node newNode = new Node(baseWorkingDir, distributionResolver, oldNode.getSpec(), RandomStringUtils.randomAlphabetic(7), true);

        nodes.set(index, newNode);
        nodeLock.unlock();

        LOGGER.info("Stopping node '{}'. It will be replaced by node '{}' on subsequent startup.", oldNode.getName(), newNode.getName());
        oldNode.stop(forcibly);

    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        upgradeNodeToVersion(index, version, false);
    }

    public void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode) {
        nodeLock.lock();
        Node oldNode = nodes.get(index);
        Node newNode = new Node(baseWorkingDir, distributionResolver, oldNode.getSpec(), RandomStringUtils.randomAlphabetic(7), true);

        LOGGER.info("Replacing node '{}' with node '{}'", oldNode.getName(), newNode.getName());
        nodes.add(index, newNode);
        newNode.start(version);
        waitUntilReady();
        oldNode.stop(forciblyDestroyOldNode);
        nodes.remove(oldNode);
        waitUntilReady();
        nodeLock.unlock();
    }

    @Override
    public void upgradeToVersion(Version version) {
        LOGGER.info("Upgrading serverless Elasticsearch cluster '{}'", name);

        List<Node> searchNodes = nodes.stream().filter(n -> n.getSpec().hasRole("search")).toList();
        List<Node> otherNodes = nodes.stream().filter(not(n -> n.getSpec().hasRole("search"))).toList();
        searchNodes.forEach(n -> upgradeNodeToVersion(n, version));
        otherNodes.forEach(n -> upgradeNodeToVersion(n, version));
    }

    public void restartNodeInPlace(int index, boolean forcibly) {
        Node node = nodes.get(index);
        node.stop(forcibly);
        node.start(null);
    }

    private void upgradeNodeToVersion(Node node, Version version) {
        upgradeNodeToVersion(nodes.indexOf(node), version);
    }
}
