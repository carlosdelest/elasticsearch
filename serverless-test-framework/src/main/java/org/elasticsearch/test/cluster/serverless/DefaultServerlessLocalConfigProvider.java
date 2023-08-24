/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.serverless;

import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

/**
 * Default configuration applied to all serverless clusters.
 */
public class DefaultServerlessLocalConfigProvider implements LocalClusterConfigProvider {

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        builder.distribution(DistributionType.DEFAULT)
            .secret("bootstrap.password", "x-pack-test-password")
            .setting("stateless.enabled", "true")
            .setting("stateless.object_store.type", "fs")
            .setting("stateless.object_store.bucket", "stateless")
            .setting("stateless.object_store.base_path", "base_path")
            .setting("ingest.geoip.downloader.enabled", "false")
            .setting("serverless.sigterm.poll_interval", "1s")
            .feature(FeatureFlag.TIME_SERIES_MODE)
            .withNode(
                indexNodeSpec -> indexNodeSpec.name("index")
                    .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                    .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                    .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            )
            .withNode(
                searchNodeSpec -> searchNodeSpec.name("search")
                    .setting("node.roles", "[remote_cluster_client,search]")
                    .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                    .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            );
    }
}
