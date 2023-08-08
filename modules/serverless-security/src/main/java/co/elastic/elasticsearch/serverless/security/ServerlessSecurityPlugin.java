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

package co.elastic.elasticsearch.serverless.security;

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
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.ServerlessApiProtections;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.INCLUDED_RESERVED_ROLES_SETTING;

/**
 * Custom rules for security, e.g. operator privileges and reserved roles, when running in the serverless environment.
 */
public class ServerlessSecurityPlugin extends Plugin implements ActionPlugin {

    // TODO: This setting should be removed in the future, and API protections will be on by default
    public static final Setting<Boolean> API_PROTECTIONS_SETTING = Setting.boolSetting(
        "http.api_protections.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private ServerlessApiProtections apiProtections;

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
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(API_PROTECTIONS_SETTING, this::configureApiProtections);

        // TODO: require operator privileges to be enabled. need to coordinate with deployment and testing concerns before we require this
        // if (OPERATOR_PRIVILEGES_ENABLED.get(environment.settings()) == false) {
        // throw new AssertionError("Operator privileges are required for serverless deployments. " +
        // "Please set [" + OPERATOR_PRIVILEGES_ENABLED.getKey() + "] to true");
        // }

        return Collections.emptyList();

    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .putList(
                INCLUDED_RESERVED_ROLES_SETTING.getKey(),
                "superuser",
                "remote_monitoring_agent",
                "remote_monitoring_collector",
                "editor",
                "viewer",
                "kibana_system"
            )
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INCLUDED_RESERVED_ROLES_SETTING, API_PROTECTIONS_SETTING);
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
        this.apiProtections = restController.getApiProtections();
        configureApiProtections(clusterSettings.get(API_PROTECTIONS_SETTING));
        return List.of();
    }

    private void configureApiProtections(boolean enabled) {
        if (this.apiProtections == null) {
            throw new IllegalStateException("API protection object not configured");
        } else {
            this.apiProtections.setEnabled(enabled);
        }
    }

    public boolean apiProtectionsEnabled() {
        return apiProtections != null && apiProtections.isEnabled();
    }
}
