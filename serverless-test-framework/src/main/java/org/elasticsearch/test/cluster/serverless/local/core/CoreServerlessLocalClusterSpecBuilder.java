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

package org.elasticsearch.test.cluster.serverless.local.core;

import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.distribution.ServerlessDistributionResolver;
import org.elasticsearch.test.cluster.serverless.local.DefaultLocalServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.local.DefaultServerlessSettingsProvider;
import org.elasticsearch.test.cluster.serverless.local.ServerlessLocalClusterFactory;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A {@link org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder} implementation used for running core or "stateful" tests against
 * a stateless cluster. This implementation has some slightly different defaults, it also strictly enforces some conventions,
 * such as having 1 search node, and 1 index node.
 *
 * This implementation isn't generally expected to be used directly. Instead, it's registered as an SPI provider that is loaded dynamically
 * when present on the classpath.
 */
public class CoreServerlessLocalClusterSpecBuilder extends AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> {
    private static final List<String> EXCLUDED_SETTINGS = List.of(
        "xpack.license.self_generated.type",
        "xpack.monitoring.collection.enabled",
        "xpack.security.enabled"
    );

    public CoreServerlessLocalClusterSpecBuilder() {
        this.distribution(DistributionType.DEFAULT)
            .settings(new DefaultServerlessSettingsProvider())
            .environment(new DefaultEnvironmentProvider())
            .setting("stateless.enabled", "true")
            .setting("stateless.object_store.type", "fs")
            .setting("stateless.object_store.bucket", "stateless")
            .setting("stateless.object_store.base_path", "base_path")
            .setting(
                "ingest.geoip.downloader.enabled",
                () -> "false",
                s -> s.getModules().contains("ingest-geoip") || s.getDistributionType() == DistributionType.DEFAULT
            )
            .setting("xpack.ml.enabled", () -> "false", s -> s.getDistributionType() == DistributionType.INTEG_TEST)
            .setting("serverless.sigterm.poll_interval", "1s")
            .user(
                System.getProperty("tests.rest.cluster.username", "stateful_rest_test_admin"),
                System.getProperty("tests.rest.cluster.password", "x-pack-test-password")
            )
            .feature(FeatureFlag.TIME_SERIES_MODE);

        this.withNode0(
            n -> n.name("index")
                .setting("node.roles", "[master,remote_cluster_client,ingest,index]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        );
        this.withNode0(
            n -> n.name("search")
                .setting("node.roles", "[remote_cluster_client,search]")
                .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
                .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        );
        this.withNode0(n -> n.name("ml").setting("node.roles", "[remote_cluster_client,ml,transform]"));
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

    @Override
    public LocalClusterSpecBuilder<ServerlessElasticsearchCluster> setting(String setting, String value) {
        // Don't apply settings we know are not applicable to serverless
        if (EXCLUDED_SETTINGS.contains(setting) == false) {
            return super.setting(setting, value);
        } else {
            return this;
        }
    }

    @Override
    public LocalClusterSpecBuilder<ServerlessElasticsearchCluster> setting(String setting, Supplier<String> value) {
        // Don't apply settings we know are not applicable to serverless
        if (EXCLUDED_SETTINGS.contains(setting) == false) {
            return super.setting(setting, value);
        } else {
            return this;
        }
    }

    @Override
    public LocalClusterSpecBuilder<ServerlessElasticsearchCluster> setting(
        String setting,
        Supplier<String> value,
        Predicate<LocalClusterSpec.LocalNodeSpec> predicate
    ) {
        // Don't apply settings we know are not applicable to serverless
        if (EXCLUDED_SETTINGS.contains(setting) == false) {
            return super.setting(setting, value, predicate);
        } else {
            return this;
        }
    }

    @Override
    public LocalClusterSpecBuilder<ServerlessElasticsearchCluster> keystore(String key, Resource file) {
        throw new UnsupportedOperationException("Non-string security settings are unsupported in serverless.");
    }

    private AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> withNode0(Consumer<? super LocalNodeSpecBuilder> config) {
        return super.withNode(config);
    }

    @Override
    public AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> nodes(int nodes) {
        throw new UnsupportedOperationException("Changing the number of nodes is not supported in serverless mode");
    }

    @Override
    public AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> withNode(Consumer<? super LocalNodeSpecBuilder> config) {
        throw new UnsupportedOperationException("Adding nodes is not supported in serverless mode");
    }

    @Override
    public AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> node(int index, Consumer<? super LocalNodeSpecBuilder> config) {
        throw new UnsupportedOperationException("Modifying existing nodes is not supported in serverless mode");
    }
}
