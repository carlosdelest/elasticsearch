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

import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
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
        return new DefaultLocalServerlessElasticsearchCluster(
            this::buildClusterSpec,
            new ServerlessLocalClusterFactory(
                new ServerlessDistributionResolver(new LocalDistributionResolver(new ReleasedDistributionResolver()))
            )
        );
    }

}
