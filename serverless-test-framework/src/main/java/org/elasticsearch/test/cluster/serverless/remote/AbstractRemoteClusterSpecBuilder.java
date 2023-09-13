/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless.remote;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRemoteClusterSpecBuilder<T extends RemoteServerlessElasticsearchCluster> extends AbstractRemoteSpecBuilder<
    RemoteClusterSpecBuilder<T>> implements RemoteClusterSpecBuilder<T> {

    protected final List<ServerlessClusterAccessProvider> clusterAccessProviders = new ArrayList<>();

    protected void clusterAccess(ServerlessClusterAccessProvider clusterAccessProvider) {
        this.clusterAccessProviders.add(clusterAccessProvider);
    }

}
