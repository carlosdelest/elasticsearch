/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.serverless.local.ServerlessLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.serverless.remote.RemoteClusterSpecBuilder;
import org.elasticsearch.test.cluster.serverless.remote.RemoteServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.remote.ServerlessRemoteClusterSpecBuilder;
import org.elasticsearch.test.cluster.util.Version;

public interface ServerlessElasticsearchCluster extends ElasticsearchCluster {

    /**
     * Creates a new {@link LocalClusterSpecBuilder} for defining a locally orchestrated cluster. Local clusters use a locally built
     * Elasticsearch distribution.
     *
     * @return a builder for a local cluster
     */
    static ServerlessLocalClusterSpecBuilder local() {
        return new ServerlessLocalClusterSpecBuilder();
    }

    /**
     * Creates a new {@link RemoteClusterSpecBuilder} for defining a remotely orchestrated cluster. Remote clusters do not
     * use a locally built Elasticsearch distribution but a distribution deployed to k8s based dev environment before.
     *
     * @return a builder for a remote cluster deployed in a platform dev environment
     */
    static RemoteClusterSpecBuilder<RemoteServerlessElasticsearchCluster> remote() {
        return new ServerlessRemoteClusterSpecBuilder();
    }

    /**
     * Upgrades a single node to the given version. Method blocks until the node is back up and ready to respond to requests.
     * When {@code forciblyDestroyOldNode} is {@code true}, the old node is forcibly killed rather than gracefully shut down. This
     * simulates a scenario in which the old node "dies" during upgrade, forcing a recovery rather than relocation on the newly upgraded
     * node.
     *
     * @param index index of node to upgrade
     * @param version version to upgrade to
     * @param forciblyDestroyOldNode whether to forcibly destroy the old node
     */
    void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode);

    /**
     * Restarts the given node with the same node id.
     * @param index index node to restart
     * @param forciblyDestroyOldNode whether to forcibly destroy the old node
     */
    void restartNodeInPlace(int index, boolean forciblyDestroyOldNode);
}
