/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.serverless.distribution.ServerlessDistributionResolver;
import org.elasticsearch.test.cluster.util.resource.Resource;

public class ServerlessLocalClusterSpecBuilder extends AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> {

    public ServerlessLocalClusterSpecBuilder() {
        this.settings(new DefaultSettingsProvider());
        this.environment(new DefaultEnvironmentProvider());
        this.apply(new DefaultServerlessLocalConfigProvider());
        this.rolesFile(Resource.fromClasspath("default_test_roles.yml"));
    }

    @Override
    public ServerlessElasticsearchCluster build() {
        return new DefaultServerlessElasticsearchCluster(
            this::buildClusterSpec,
            new ServerlessLocalClusterFactory(
                new ServerlessDistributionResolver(new LocalDistributionResolver(new ReleasedDistributionResolver()))
            )
        );
    }

}
