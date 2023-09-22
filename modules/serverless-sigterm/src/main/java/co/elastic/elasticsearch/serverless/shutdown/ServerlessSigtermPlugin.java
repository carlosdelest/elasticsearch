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

package co.elastic.elasticsearch.serverless.shutdown;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.shutdown.NodeSeenService;
import org.elasticsearch.xpack.shutdown.RestGetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Replaces the Node Shutdown plugin to make it work as desired for Serverless, which is primarily converting how k8s wants to talk to
 * Elasticsearch (SIGTERM, you're welcome) into Node Shutdown requests.
 */
public class ServerlessSigtermPlugin extends ShutdownPlugin {
    private SigtermTerminationHandler terminationHandler;
    /**
     * The maximum amount of time to wait for an orderly shutdown.
     */
    public static final Setting<TimeValue> TIMEOUT_SETTING = Setting.timeSetting(
        "serverless.sigterm.timeout",
        new TimeValue(1, TimeUnit.HOURS),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.sigterm.poll_interval",
        new TimeValue(1, TimeUnit.MINUTES),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

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
        TelemetryProvider telemetryProvider,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        this.terminationHandler = new SigtermTerminationHandler(
            client,
            threadPool,
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()),
            TIMEOUT_SETTING.get(clusterService.getSettings()),
            nodeEnvironment.nodeId()
        );

        NodeSeenService nodeSeenService = new NodeSeenService(clusterService);
        SigtermShutdownCleanupService shutdownCleanupService = new SigtermShutdownCleanupService(clusterService);
        return List.of(nodeSeenService, shutdownCleanupService);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        // Nothing should be calling the Put or Delete Shutdown APIs from the REST layer, so don't register them.
        // Exposing Get Status internally will be useful for troubleshooting.
        return Collections.singletonList(new RestGetShutdownStatusAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TIMEOUT_SETTING, POLL_INTERVAL_SETTING);
    }

    TerminationHandler getTerminationHandler() {
        return this.terminationHandler;
    }
}
