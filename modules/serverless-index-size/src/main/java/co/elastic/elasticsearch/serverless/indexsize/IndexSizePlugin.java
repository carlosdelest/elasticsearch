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

package co.elastic.elasticsearch.serverless.indexsize;

import co.elastic.elasticsearch.serverless.indexsize.action.CollectMeteringShardInfoAction;
import co.elastic.elasticsearch.serverless.indexsize.action.GetMeteringShardInfoAction;
import co.elastic.elasticsearch.serverless.indexsize.action.TransportCollectMeteringShardInfoAction;
import co.elastic.elasticsearch.serverless.indexsize.action.TransportGetMeteringShardInfoAction;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.Collection;
import java.util.List;

public class IndexSizePlugin extends Plugin implements PersistentTaskPlugin, ActionPlugin {

    static final NodeFeature INDEX_SIZE_SUPPORTED = new NodeFeature("index_size.supported");

    private IndexSizeTaskExecutor indexSizeTaskExecutor;
    private MeteringShardInfoService meteringShardInfoService;
    private final boolean hasSearchRole;

    public IndexSizePlugin(Settings settings) {
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(IndexSizeTaskExecutor.ENABLED_SETTING, IndexSizeTaskExecutor.POLL_INTERVAL_SETTING);
    }

    @Override
    public Collection<?> createComponents(Plugin.PluginServices services) {
        var indexSizeService = new IndexSizeService();

        // TODO[lor]: We should not create multiple PersistentTasksService. Instead, we should create one in Server and pass it to plugins
        // via services or via PersistentTaskPlugin#getPersistentTasksExecutor. See elasticsearch#105662
        var persistentTasksService = new PersistentTasksService(services.clusterService(), services.threadPool(), services.client());

        indexSizeTaskExecutor = IndexSizeTaskExecutor.create(
            services.client(),
            services.clusterService(),
            persistentTasksService,
            services.featureService(),
            services.threadPool(),
            indexSizeService,
            services.environment().settings()
        );
        if (hasSearchRole) {
            meteringShardInfoService = new MeteringShardInfoService();
            return List.of(indexSizeTaskExecutor, indexSizeService, meteringShardInfoService);
        } else {
            meteringShardInfoService = null;
        }
        return List.of(indexSizeTaskExecutor, indexSizeService);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(indexSizeTaskExecutor);
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(IndexSizeTask.TASK_NAME),
                IndexSizeTaskParams::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, IndexSizeTask.TASK_NAME, reader -> IndexSizeTaskParams.INSTANCE)
        );
    }

    @Override
    public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var collectShardSize = new ActionHandler<>(CollectMeteringShardInfoAction.INSTANCE, TransportCollectMeteringShardInfoAction.class);
        if (hasSearchRole) {
            return List.of(
                new ActionPlugin.ActionHandler<>(GetMeteringShardInfoAction.INSTANCE, TransportGetMeteringShardInfoAction.class),
                collectShardSize
            );
        } else {
            return List.of(collectShardSize);
        }
    }
}
