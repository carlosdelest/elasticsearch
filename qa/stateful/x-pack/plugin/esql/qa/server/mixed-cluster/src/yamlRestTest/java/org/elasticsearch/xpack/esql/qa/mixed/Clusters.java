/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessBwcVersion;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;

public class Clusters {
    public static ElasticsearchCluster mixedVersionCluster() {
        return ServerlessElasticsearchCluster.local()
            .version(ServerlessBwcVersion.instance())
            .withNode(
                n -> n.name("index-2")
                    .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                    .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                    .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            )
            .withNode(
                n -> n.name("search-2")
                    .version(Version.CURRENT)
                    .setting("node.roles", "[remote_cluster_client,search]")
                    .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                    .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            )
            .user(
                System.getProperty("tests.rest.cluster.username", "stateful_rest_test_admin"),
                System.getProperty("tests.rest.cluster.password", "x-pack-test-password")
            )
            .setting("cluster.routing.rebalance.enable", "none") // disable relocation until we have retry in ESQL
            // TODO: remove after serverless release
            .jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper")
            .jvmArg("-da:org.elasticsearch.index.mapper.MapperService")
            .build();
    }
}
