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

package co.elastic.elasticsearch.serverless.crossproject;

import co.elastic.elasticsearch.serverless.crossproject.action.TransportGetProjectTagsAction;
import co.elastic.elasticsearch.serverless.crossproject.rest.action.RestGetProjectTagsAction;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

public class ServerlessCrossProjectPlugin extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> CROSS_PROJECT_ENABLED = Setting.boolSetting(
        "serverless.cross_project.enabled",
        false,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ServerlessCrossProjectPlugin.class);

    private final boolean crossProjectEnabled;
    private final boolean hasSearchRole;

    public ServerlessCrossProjectPlugin(Settings settings) {
        crossProjectEnabled = CROSS_PROJECT_ENABLED.get(settings);
        logger.info("cross-project is [{}]", crossProjectEnabled ? "enabled" : "disabled");
        hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    public boolean isCrossProjectEnabled() {
        return crossProjectEnabled;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(CROSS_PROJECT_ENABLED);
    }

    @Override
    public Settings additionalSettings() {
        if (crossProjectEnabled == false || hasSearchRole == false) {
            return Settings.EMPTY;
        }

        logger.info("hasSearchRole is [true], setting [{}] to [true] in additionalSettings()", REMOTE_CLUSTER_SERVER_ENABLED.getKey());
        return Settings.builder().put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true).build();
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (crossProjectEnabled) {
            return List.of(new ActionHandler(TransportGetProjectTagsAction.INSTANCE, TransportGetProjectTagsAction.class));
        } else {
            return List.of();
        }
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (crossProjectEnabled) {
            return List.of(new RestGetProjectTagsAction());
        } else {
            return List.of();
        }
    }
}
