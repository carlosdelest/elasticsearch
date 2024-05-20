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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.action.CollectMeteringShardInfoAction;
import co.elastic.elasticsearch.metering.action.GetMeteringShardInfoAction;
import co.elastic.elasticsearch.metering.action.GetMeteringStatsAction;
import co.elastic.elasticsearch.metering.action.LocalNodeMeteringShardInfoCache;
import co.elastic.elasticsearch.metering.action.MeteringIndexInfoService;
import co.elastic.elasticsearch.metering.action.TransportCollectMeteringShardInfoAction;
import co.elastic.elasticsearch.metering.action.TransportGetMeteringShardInfoAction;
import co.elastic.elasticsearch.metering.action.TransportGetMeteringStatsForPrimaryUserAction;
import co.elastic.elasticsearch.metering.action.TransportGetMeteringStatsForSecondaryUserAction;
import co.elastic.elasticsearch.metering.ingested_size.MeteringDocumentParsingProvider;
import co.elastic.elasticsearch.metering.reports.MeteringReporter;
import co.elastic.elasticsearch.metering.stats.rest.RestGetMeteringStatsAction;
import co.elastic.elasticsearch.metrics.CounterMetricsCollector;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;
import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentParsingProviderPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_ID;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;

/**
 * Plugin responsible for starting up all serverless metering classes.
 */
public class MeteringPlugin extends Plugin implements ExtensiblePlugin, DocumentParsingProviderPlugin, PersistentTaskPlugin, ActionPlugin {

    private static final Logger log = LogManager.getLogger(MeteringPlugin.class);

    private static final String METERING_REPORTER_THREAD_POOL_NAME = "metering_reporter";
    static final NodeFeature INDEX_INFO_SUPPORTED = new NodeFeature("index_size.supported");

    public static final Setting<Boolean> NEW_IX_METRIC_SETTING = Setting.boolSetting(
        "metering.new-index-size.enabled",
        false,
        Setting.Property.NodeScope
    );
    private final ProjectType projectType;

    private MeteringIndexInfoTaskExecutor meteringIndexInfoTaskExecutor;
    private final boolean hasSearchRole;
    private List<SampledMetricsCollector> sampledMetricsCollectors;
    private List<CounterMetricsCollector> counterMetricsCollectors;
    private MeteringReporter reporter;
    private MeteringReportingService reportingService;

    private volatile IngestMetricsCollector ingestMetricsCollector;
    private volatile SystemIndices systemIndices;

    public MeteringPlugin(Settings settings) {
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        this.projectType = PROJECT_TYPE.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            MeteringReportingService.REPORT_PERIOD,
            MeteringReporter.METERING_URL,
            MeteringReporter.BATCH_SIZE,
            MeteringIndexInfoTaskExecutor.ENABLED_SETTING,
            MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING,
            NEW_IX_METRIC_SETTING
        );
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        sampledMetricsCollectors = loader.loadExtensions(SampledMetricsCollector.class);
        counterMetricsCollectors = loader.loadExtensions(CounterMetricsCollector.class);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                METERING_REPORTER_THREAD_POOL_NAME,
                1,
                1,
                "serverless.metering.reporter.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.systemIndices = services.systemIndices();
        ClusterService clusterService = services.clusterService();
        ThreadPool threadPool = services.threadPool();
        Environment environment = services.environment();
        NodeEnvironment nodeEnvironment = services.nodeEnvironment();

        String projectId = PROJECT_ID.get(environment.settings());
        log.info("Initializing MeteringPlugin using node id [{}], project id [{}]", nodeEnvironment.nodeId(), projectId);

        boolean useNewIndexSizeMetric = NEW_IX_METRIC_SETTING.get(environment.settings());
        log.info("Using new IX metric: [{}]", useNewIndexSizeMetric);

        ingestMetricsCollector = new IngestMetricsCollector(
            nodeEnvironment.nodeId(),
            clusterService.getClusterSettings(),
            environment.settings()
        );

        var indexSizeService = new MeteringIndexInfoService();
        clusterService.addListener(indexSizeService);

        List<CounterMetricsCollector> builtInCounterMetrics = new ArrayList<>();
        List<SampledMetricsCollector> builtInSampledMetrics = new ArrayList<>();
        List<DiscoveryNodeRole> discoveryNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(environment.settings());
        if (discoveryNodeRoles.contains(DiscoveryNodeRole.INGEST_ROLE) || discoveryNodeRoles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
            builtInCounterMetrics.add(ingestMetricsCollector);
        }

        TimeValue reportPeriod = MeteringReportingService.REPORT_PERIOD.get(environment.settings());

        // TODO[ES-7639]: remove this choice when migration to IX in ES is completed
        if (useNewIndexSizeMetric) {
            builtInSampledMetrics.add(indexSizeService.createIndexSizeMetricsCollector(clusterService, environment.settings()));
        } else {
            if (discoveryNodeRoles.contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                builtInSampledMetrics.add(
                    new IndexSizeMetricsCollector(services.indicesService(), clusterService.getClusterSettings(), environment.settings())
                );
            }
        }

        builtInCounterMetrics.addAll(counterMetricsCollectors);
        builtInSampledMetrics.addAll(sampledMetricsCollectors);

        if (projectId.isEmpty()) {
            log.warn(PROJECT_ID.getKey() + " is not set, metric reporting is disabled");
        } else {
            reporter = new MeteringReporter(environment.settings(), threadPool);
        }

        reportingService = new MeteringReportingService(
            nodeEnvironment.nodeId(),
            projectId,
            builtInCounterMetrics,
            builtInSampledMetrics,
            reportPeriod,
            reporter != null ? reporter::sendRecords : records -> {},
            threadPool,
            threadPool.executor(METERING_REPORTER_THREAD_POOL_NAME)
        );

        List<Object> cs = new ArrayList<>();
        if (reporter != null) {
            cs.add(reporter);
        }
        cs.add(reportingService);
        cs.add(indexSizeService);
        cs.addAll(builtInCounterMetrics);
        cs.addAll(builtInSampledMetrics);

        // TODO[lor]: We should not create multiple PersistentTasksService. Instead, we should create one in Server and pass it to plugins
        // via services or via PersistentTaskPlugin#getPersistentTasksExecutor. See elasticsearch#105662
        var persistentTasksService = new PersistentTasksService(clusterService, threadPool, services.client());

        meteringIndexInfoTaskExecutor = MeteringIndexInfoTaskExecutor.create(
            services.client(),
            clusterService,
            persistentTasksService,
            services.featureService(),
            threadPool,
            indexSizeService,
            environment.settings()
        );
        cs.add(meteringIndexInfoTaskExecutor);
        if (hasSearchRole) {
            cs.add(new LocalNodeMeteringShardInfoCache());
        }

        return cs;
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
        return List.of(new RestGetMeteringStatsAction());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(reporter, reportingService);
    }

    IngestMetricsCollector getIngestMetricsCollector() {
        return ingestMetricsCollector;
    }

    SystemIndices getSystemIndices() {
        return systemIndices;
    }

    /**
     * This method is called during node construction to allow for injection.
     * The DocumentParsingProvider instance depends on ingestMetricsCollector created during {@link #createComponents}.
     * The DocumentParsingProvider instance is being used after the {@link #createComponents}, therefore ingestMetricsCollector
     * should be stored in a volatile field.
     */
    @Override
    public DocumentParsingProvider getDocumentParsingProvider() {
        return new MeteringDocumentParsingProvider(projectType, this::getIngestMetricsCollector, this::getSystemIndices);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(meteringIndexInfoTaskExecutor);
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(MeteringIndexInfoTask.TASK_NAME),
                MeteringIndexInfoTaskParams::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                MeteringIndexInfoTask.TASK_NAME,
                reader -> MeteringIndexInfoTaskParams.INSTANCE
            )
        );
    }

    @Override
    public Collection<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var collectMeteringShardInfo = new ActionPlugin.ActionHandler<>(
            CollectMeteringShardInfoAction.INSTANCE,
            TransportCollectMeteringShardInfoAction.class
        );
        var getMeteringStatsSecondaryUser = new ActionPlugin.ActionHandler<>(
            GetMeteringStatsAction.FOR_SECONDARY_USER_INSTANCE,
            TransportGetMeteringStatsForSecondaryUserAction.class
        );
        var getMeteringStatsPrimaryUser = new ActionPlugin.ActionHandler<>(
            GetMeteringStatsAction.FOR_PRIMARY_USER_INSTANCE,
            TransportGetMeteringStatsForPrimaryUserAction.class
        );
        if (hasSearchRole) {
            var getMeteringShardInfo = new ActionPlugin.ActionHandler<>(
                GetMeteringShardInfoAction.INSTANCE,
                TransportGetMeteringShardInfoAction.class
            );
            return List.of(getMeteringShardInfo, getMeteringStatsSecondaryUser, getMeteringStatsPrimaryUser, collectMeteringShardInfo);
        } else {
            return List.of(getMeteringStatsSecondaryUser, getMeteringStatsPrimaryUser, collectMeteringShardInfo);
        }
    }
}
