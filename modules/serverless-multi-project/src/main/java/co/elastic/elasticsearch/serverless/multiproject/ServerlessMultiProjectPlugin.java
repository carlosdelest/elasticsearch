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

package co.elastic.elasticsearch.serverless.multiproject;

import co.elastic.elasticsearch.serverless.multiproject.action.DeleteProjectAction;
import co.elastic.elasticsearch.serverless.multiproject.action.PutProjectAction;
import co.elastic.elasticsearch.serverless.multiproject.action.RestDeleteProjectAction;
import co.elastic.elasticsearch.serverless.multiproject.action.RestPutProjectAction;

import org.apache.lucene.util.SetOnce;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.tasks.Task;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ServerlessMultiProjectPlugin extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> MULTI_PROJECT_ENABLED = Setting.boolSetting(
        "serverless.multi_project.enabled",
        false,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ServerlessMultiProjectPlugin.class);

    private final boolean multiProjectEnabled;
    public final SetOnce<ThreadContext> threadContext = new SetOnce<>();

    public ServerlessMultiProjectPlugin(Settings settings) {
        /*
         * Ideally we would resolve settings inside `createComponents` because the settings can change between
         * plugin construction time and component creation time (e.g. using `additionalSettings` in this, or another plugin).
         * However, the `ProjectResolver` is loaded (via SPI) before `createComponents` is called, so we have to rely on the
         * value that is set at construction time.
         */
        multiProjectEnabled = MULTI_PROJECT_ENABLED.get(settings);
        logger.info("multi-project is [{}]", multiProjectEnabled ? "enabled" : "disabled");
    }

    public boolean isMultiProjectEnabled() {
        return multiProjectEnabled;
    }

    public ThreadContext getThreadContext() {
        return threadContext.get();
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        if (multiProjectEnabled) {
            return Set.of(new RestHeaderDefinition(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, false));
        } else {
            return Set.of();
        }
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.threadContext.set(services.threadPool().getThreadContext());
        return List.of();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MULTI_PROJECT_ENABLED);
    }

    @Override
    public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (multiProjectEnabled) {
            return List.of(
                new ActionHandler<>(PutProjectAction.INSTANCE, PutProjectAction.TransportPutProjectAction.class),
                new ActionHandler<>(DeleteProjectAction.INSTANCE, DeleteProjectAction.TransportDeleteProjectAction.class)
            );
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
        if (multiProjectEnabled) {
            return List.of(new RestPutProjectAction(), new RestDeleteProjectAction());
        } else {
            return List.of();
        }
    }

}
