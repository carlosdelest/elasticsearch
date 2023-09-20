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

package co.elastic.elasticsearch.ml.serverless;

import co.elastic.elasticsearch.ml.serverless.actionfilters.GetDataFrameAnalyticsStatsResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.GetDatafeedStatsResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.GetJobModelSnapshotsUpgradeStatsResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.GetJobStatsResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.GetTrainedModelsStatsResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.NodeAcknowledgedResponseFilter;
import co.elastic.elasticsearch.ml.serverless.actionfilters.UpgradeJobModelSnapshotResponseFilter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class ServerlessMachineLearningPlugin extends Plugin implements ActionPlugin {

    public static final String NAME = "serverless-ml";

    // These 3 settings enable parts of ML to be enabled or disabled at a more granular level than the entire plugin
    public static final Setting<Boolean> ANOMALY_DETECTION_ENABLED = Setting.boolSetting("xpack.ml.ad.enabled", true, Property.NodeScope);
    public static final Setting<Boolean> DATA_FRAME_ANALYTICS_ENABLED = Setting.boolSetting(
        "xpack.ml.dfa.enabled",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> NLP_ENABLED = Setting.boolSetting("xpack.ml.nlp.enabled", true, Property.NodeScope);

    public final SetOnce<List<ActionFilter>> actionFilters = new SetOnce<>();

    public ServerlessMachineLearningPlugin() {}

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
        ThreadContext threadContext = threadPool.getThreadContext();
        actionFilters.set(
            List.of(
                new NodeAcknowledgedResponseFilter(threadContext, OpenJobAction.NAME),
                new NodeAcknowledgedResponseFilter(threadContext, StartDatafeedAction.NAME),
                new NodeAcknowledgedResponseFilter(threadContext, StartDataFrameAnalyticsAction.NAME),
                new UpgradeJobModelSnapshotResponseFilter(threadContext),
                new GetDatafeedStatsResponseFilter(threadContext),
                new GetDataFrameAnalyticsStatsResponseFilter(threadContext),
                new GetJobStatsResponseFilter(threadContext),
                new GetJobModelSnapshotsUpgradeStatsResponseFilter(threadContext),
                new GetTrainedModelsStatsResponseFilter(threadContext)
            )
        );

        return List.of();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ANOMALY_DETECTION_ENABLED, DATA_FRAME_ANALYTICS_ENABLED, NLP_ENABLED);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return actionFilters.get();
    }
}
