/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.test.cluster.ClusterFactory;
import org.elasticsearch.test.cluster.DefaultElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.function.Supplier;

public class DefaultServerlessElasticsearchCluster extends DefaultElasticsearchCluster<LocalClusterSpec, ServerlessLocalClusterHandle>
    implements
        ServerlessElasticsearchCluster {

    public DefaultServerlessElasticsearchCluster(
        Supplier<LocalClusterSpec> specProvider,
        ClusterFactory<LocalClusterSpec, ServerlessLocalClusterHandle> clusterFactory
    ) {
        super(specProvider, clusterFactory);
    }
}
