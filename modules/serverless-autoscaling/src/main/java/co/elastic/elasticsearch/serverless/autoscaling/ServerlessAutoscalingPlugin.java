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

package co.elastic.elasticsearch.serverless.autoscaling;

import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetIndexTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetMachineLearningTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetSearchTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.TransportGetAutoscalingMetricsAction;
import co.elastic.elasticsearch.serverless.autoscaling.action.TransportGetIndexTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.TransportGetMachineLearningTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.TransportGetSearchTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.rest.action.RestGetAutoscalingMetricsAction;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.autoscaling.action.TransportGetAutoscalingMetricsAction.AUTOSCALING_METRICS_ENABLED_SETTING;

/**
 * Plugin that provides actions for autoscaling
 */
public class ServerlessAutoscalingPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "serverless-autoscaling";

    public ServerlessAutoscalingPlugin(Settings settings) {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(GetAutoscalingMetricsAction.INSTANCE, TransportGetAutoscalingMetricsAction.class),
            new ActionHandler<>(GetSearchTierMetrics.INSTANCE, TransportGetSearchTierMetrics.class),
            new ActionHandler<>(GetIndexTierMetrics.INSTANCE, TransportGetIndexTierMetrics.class),
            new ActionHandler<>(GetMachineLearningTierMetrics.INSTANCE, TransportGetMachineLearningTierMetrics.class)
        );
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
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestGetAutoscalingMetricsAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(AUTOSCALING_METRICS_ENABLED_SETTING);
    }
}
