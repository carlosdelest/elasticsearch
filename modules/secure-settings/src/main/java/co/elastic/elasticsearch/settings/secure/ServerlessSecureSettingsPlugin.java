/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.settings.secure;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class ServerlessSecureSettingsPlugin extends Plugin implements ReloadAwarePlugin {

    private ClusterStateSecretsListener clusterStateSecretsListener;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService
    ) {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(clusterService, environment);
        this.clusterStateSecretsListener = new ClusterStateSecretsListener(clusterService, environment);
        return List.of(fileSecureSettingsService, clusterStateSecretsListener);
    }

    @Override
    public void setReloadCallback(ReloadablePlugin reloadablePlugin) {
        this.clusterStateSecretsListener.setReloadCallback(reloadablePlugin);
    }

    /**
     * Returns parsers for {@link NamedWriteable} this plugin will use over the transport protocol.
     * @see NamedWriteableRegistry
     */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, ClusterStateSecrets.TYPE, ClusterStateSecrets::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ClusterStateSecrets.TYPE, ClusterStateSecrets::readDiffFrom),
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::readDiffFrom)
        );
    }
}
