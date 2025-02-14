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

import co.elastic.elasticsearch.metering.action.GetMeteringStatsAction;
import co.elastic.elasticsearch.metering.action.TransportGetMeteringStatsForPrimaryUserAction;
import co.elastic.elasticsearch.metering.action.TransportGetMeteringStatsForSecondaryUserAction;
import co.elastic.elasticsearch.metering.activitytracking.ActivityTrackerActionFilter;
import co.elastic.elasticsearch.metering.activitytracking.DefaultActionTierMapper;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTask;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskExecutor;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskParams;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService;
import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metering.sampling.action.GetNodeSamplesAction;
import co.elastic.elasticsearch.metering.sampling.action.TransportCollectClusterSamplesAction;
import co.elastic.elasticsearch.metering.sampling.action.TransportGetNodeSamplesAction;
import co.elastic.elasticsearch.metering.stats.rest.RestGetMeteringStatsAction;
import co.elastic.elasticsearch.metering.usagereports.UsageReportService;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metering.usagereports.action.TransportUpdateSampledMetricsMetadataAction;
import co.elastic.elasticsearch.metering.usagereports.action.UpdateSampledMetricsMetadataAction;
import co.elastic.elasticsearch.metering.usagereports.publisher.HttpMeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.usagereports.publisher.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.xcontent.MeteringDocumentParsingProvider;
import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;
import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SafeClusterStateSupplier;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_ID;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;

/**
 * Plugin responsible for starting up all serverless metering classes.
 */
public class MeteringPlugin extends Plugin implements DocumentParsingProviderPlugin, PersistentTaskPlugin, ActionPlugin, MapperPlugin {

    private static final Logger log = LogManager.getLogger(MeteringPlugin.class);

    private static final String METERING_REPORTER_THREAD_POOL_NAME = "metering_reporter";

    private final ProjectType projectType;

    private SampledClusterMetricsSchedulingTaskExecutor clusterMetricsSchedulingTaskExecutor;
    private final boolean hasIndexRole;
    public final SetOnce<List<ActionFilter>> actionFilters = new SetOnce<>();
    private volatile IngestMetricsProvider ingestMetricsProvider;

    public MeteringPlugin(Settings settings) {
        this.hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
        this.projectType = PROJECT_TYPE.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            UsageReportService.REPORT_PERIOD,
            HttpMeteringUsageRecordPublisher.METERING_URL,
            HttpMeteringUsageRecordPublisher.BATCH_SIZE,
            HttpMeteringUsageRecordPublisher.REQUEST_TIMEOUT,
            SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING,
            SampledClusterMetricsSchedulingTaskExecutor.POLL_INTERVAL_SETTING,
            TaskActivityTracker.COOL_DOWN_PERIOD
        );
    }

    @Override
    public Collection<ActionFilter> getActionFilters() {
        return actionFilters.get();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                METERING_REPORTER_THREAD_POOL_NAME,
                1,
                2,
                "serverless.metering.reporter.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ClusterService clusterService = services.clusterService();
        SystemIndices systemIndices = services.systemIndices();
        ThreadPool threadPool = services.threadPool();
        Environment environment = services.environment();
        NodeEnvironment nodeEnvironment = services.nodeEnvironment();

        var hasSearchRole = DiscoveryNode.hasRole(environment.settings(), DiscoveryNodeRole.SEARCH_ROLE);
        var hasPersistentTasksRole = DiscoveryNode.hasRole(environment.settings(), SampledClusterMetricsSchedulingTask.ASSIGNED_ROLE);

        String projectId = PROJECT_ID.get(environment.settings());
        log.info("Initializing MeteringPlugin using node id [{}], project id [{}]", nodeEnvironment.nodeId(), projectId);

        var clusterStateSupplier = new SafeClusterStateSupplier();
        clusterService.addListener(clusterStateSupplier);

        var clusterMetricsService = new SampledClusterMetricsService(clusterService, services.telemetryProvider().getMeterRegistry());

        var activityTracker = TaskActivityTracker.build(
            Clock.systemUTC(),
            Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis()),
            hasSearchRole,
            hasIndexRole,
            threadPool.getThreadContext(),
            DefaultActionTierMapper.INSTANCE,
            services.taskManager()
        );
        actionFilters.set(List.of(new ActivityTrackerActionFilter(activityTracker)));

        List<Object> cs = new ArrayList<>();
        cs.add(clusterMetricsService);
        cs.add(activityTracker);

        List<SampledMetricsProvider> sampledMetrics = new ArrayList<>();
        List<CounterMetricsProvider> counterMetrics = new ArrayList<>();

        if (hasPersistentTasksRole) {
            // required on search nodes only according to the persistent task node assignment, see
            // SampledClusterMetricsSchedulingTaskExecutor#getNodeAssignment
            sampledMetrics.add(clusterMetricsService.createSampledStorageMetricsProvider(systemIndices));
            sampledMetrics.add(clusterMetricsService.createSampledVCUMetricsProvider(nodeEnvironment, systemIndices));
        } else if (hasIndexRole) {
            // ingest metrics are only available on index nodes
            ingestMetricsProvider = new IngestMetricsProvider(nodeEnvironment.nodeId(), clusterStateSupplier, systemIndices);
            counterMetrics.add(ingestMetricsProvider);
        }

        // on nodes other than search or index nodes, there's nothing to report
        if (sampledMetrics.isEmpty() == false || counterMetrics.isEmpty() == false) {
            MeteringUsageRecordPublisher usageRecordPublisher;
            if (projectId.isEmpty()) {
                log.warn(PROJECT_ID.getKey() + " is not set, metric reporting is disabled");
                usageRecordPublisher = MeteringUsageRecordPublisher.NOOP_REPORTER;
            } else {
                usageRecordPublisher = new HttpMeteringUsageRecordPublisher(
                    environment,
                    environment.settings(),
                    services.telemetryProvider().getMeterRegistry()
                );
            }

            TimeValue reportPeriod = UsageReportService.REPORT_PERIOD.get(environment.settings());
            UsageReportService usageReportService = new UsageReportService(
                nodeEnvironment.nodeId(),
                projectId,
                List.copyOf(counterMetrics),
                List.copyOf(sampledMetrics),
                clusterStateSupplier,
                clusterService.getSettings(),
                services.featureService(),
                services.client(),
                reportPeriod,
                usageRecordPublisher,
                threadPool,
                threadPool.executor(METERING_REPORTER_THREAD_POOL_NAME),
                services.telemetryProvider().getMeterRegistry()
            );

            cs.add(usageReportService);
        }

        // TODO[lor]: We should not create multiple PersistentTasksService. Instead, we should create one in Server and pass it to plugins
        // via services or via PersistentTaskPlugin#getPersistentTasksExecutor. See elasticsearch#105662
        var persistentTasksService = new PersistentTasksService(clusterService, threadPool, services.client());

        clusterMetricsSchedulingTaskExecutor = SampledClusterMetricsSchedulingTaskExecutor.create(
            services.client(),
            clusterService,
            persistentTasksService,
            threadPool,
            clusterMetricsService,
            environment.settings()
        );
        cs.add(clusterMetricsSchedulingTaskExecutor);
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

    public static boolean isRawStorageMeteringEnabled(ProjectType projectType) {
        return projectType == ProjectType.OBSERVABILITY || projectType == ProjectType.SECURITY;
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(RawStorageMetadataFieldMapper.FIELD_NAME, RawStorageMetadataFieldMapper.PARSER);
    }

    /**
     * This method is called during node construction to allow for injection.
     * The DocumentParsingProvider instance depends on ingestMetricsCollector created during {@link #createComponents}.
     * The DocumentParsingProvider instance is being used after the {@link #createComponents}, therefore ingestMetricsCollector
     * should be stored in a volatile field.
     */
    @Override
    public DocumentParsingProvider getDocumentParsingProvider() {
        return hasIndexRole
            ? new MeteringDocumentParsingProvider(isRawStorageMeteringEnabled(projectType), () -> ingestMetricsProvider)
            : DocumentParsingProvider.EMPTY_INSTANCE;
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(clusterMetricsSchedulingTaskExecutor);
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(SampledClusterMetricsSchedulingTask.TASK_NAME),
                SampledClusterMetricsSchedulingTaskParams::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                SampledClusterMetricsSchedulingTask.TASK_NAME,
                reader -> SampledClusterMetricsSchedulingTaskParams.INSTANCE
            ),
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, SampledMetricsMetadata.TYPE, SampledMetricsMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, SampledMetricsMetadata.TYPE, SampledMetricsMetadata::readDiffFrom)
        );
    }

    @Override
    public Collection<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var collectMeteringShardInfo = new ActionPlugin.ActionHandler<>(
            CollectClusterSamplesAction.INSTANCE,
            TransportCollectClusterSamplesAction.class
        );
        var getMeteringStatsSecondaryUser = new ActionPlugin.ActionHandler<>(
            GetMeteringStatsAction.FOR_SECONDARY_USER_INSTANCE,
            TransportGetMeteringStatsForSecondaryUserAction.class
        );
        var getMeteringStatsPrimaryUser = new ActionPlugin.ActionHandler<>(
            GetMeteringStatsAction.FOR_PRIMARY_USER_INSTANCE,
            TransportGetMeteringStatsForPrimaryUserAction.class
        );
        var updateSampledMetricsMetadata = new ActionPlugin.ActionHandler<>(
            UpdateSampledMetricsMetadataAction.INSTANCE,
            TransportUpdateSampledMetricsMetadataAction.class
        );
        var getNodeSamples = new ActionPlugin.ActionHandler<>(GetNodeSamplesAction.INSTANCE, TransportGetNodeSamplesAction.class);
        return List.of(
            getNodeSamples,
            getMeteringStatsSecondaryUser,
            getMeteringStatsPrimaryUser,
            collectMeteringShardInfo,
            updateSampledMetricsMetadata
        );
    }
}
