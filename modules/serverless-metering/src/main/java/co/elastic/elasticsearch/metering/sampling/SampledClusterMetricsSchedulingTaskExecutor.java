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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.MeteringFeatures;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Persistent task executor that is managing the {@link SampledClusterMetricsSchedulingTask}.
 */
public final class SampledClusterMetricsSchedulingTaskExecutor extends PersistentTasksExecutor<SampledClusterMetricsSchedulingTaskParams> {

    private static final Logger logger = LogManager.getLogger(SampledClusterMetricsSchedulingTaskExecutor.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "metering.index-info-task.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static TimeValue MINIMUM_METERING_INFO_UPDATE_PERIOD = TimeValue.timeValueSeconds(1);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "metering.index-info-task.poll.interval",
        TimeValue.timeValueSeconds(30),
        MINIMUM_METERING_INFO_UPDATE_PERIOD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Client client;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final ThreadPool threadPool;
    private final SampledClusterMetricsService clusterMetricsService;

    // Holds a reference to the task. This will have a valid value only on the executor node, otherwise it will be null.
    private final AtomicReference<SampledClusterMetricsSchedulingTask> executorNodeTask = new AtomicReference<>();
    private final PersistentTasksService persistentTasksService;
    private volatile boolean enabled;
    private volatile TimeValue pollInterval;

    private SampledClusterMetricsSchedulingTaskExecutor(
        Client client,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        ThreadPool threadPool,
        SampledClusterMetricsService clusterMetricsService,
        Settings settings
    ) {
        super(SampledClusterMetricsSchedulingTask.TASK_NAME, threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.client = client;

        this.clusterService = clusterService;
        this.featureService = featureService;
        this.threadPool = threadPool;
        this.clusterMetricsService = clusterMetricsService;
        this.persistentTasksService = persistentTasksService;
        this.enabled = ENABLED_SETTING.get(settings);
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
    }

    private static void registerListeners(
        ClusterService clusterService,
        ClusterSettings clusterSettings,
        SampledClusterMetricsSchedulingTaskExecutor executor
    ) {
        clusterService.addListener(executor::startStopTask);
        clusterService.addListener(executor::shuttingDown);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, executor::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, executor::updatePollInterval);
    }

    public static SampledClusterMetricsSchedulingTaskExecutor create(
        Client client,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        ThreadPool threadPool,
        SampledClusterMetricsService clusterMetricsService,
        Settings settings
    ) {
        var executor = new SampledClusterMetricsSchedulingTaskExecutor(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            clusterMetricsService,
            settings
        );
        registerListeners(clusterService, clusterService.getClusterSettings(), executor);
        return executor;
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        SampledClusterMetricsSchedulingTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        // Require the sampling task to run on a search node. This is a requirement to calculate the storage_ram_ratio,
        // which is part of the SPmin provisioned memory calculation for the search tier.
        DiscoveryNode discoveryNode = selectLeastLoadedNode(
            clusterState,
            candidateNodes,
            node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName())
        );
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new PersistentTasksCustomMetadata.Assignment(discoveryNode.getId(), "");
        }
    }

    @Override
    protected void nodeOperation(
        AllocatedPersistentTask task,
        SampledClusterMetricsSchedulingTaskParams params,
        PersistentTaskState state
    ) {
        SampledClusterMetricsSchedulingTask clusterMetricsSchedulingTask = (SampledClusterMetricsSchedulingTask) task;
        executorNodeTask.set(clusterMetricsSchedulingTask);
        DiscoveryNode node = clusterService.localNode();
        logger.info("Node [{{}}{{}}] is selected as the current sampling node.", node.getName(), node.getId());
        if (this.enabled) {
            clusterMetricsSchedulingTask.run();
        }
    }

    @Override
    protected SampledClusterMetricsSchedulingTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<SampledClusterMetricsSchedulingTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        logger.debug("Creating MeteringIndexInfoTask [{}][{}]", type, action);
        return new SampledClusterMetricsSchedulingTask(
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            threadPool,
            clusterMetricsService,
            client,
            () -> pollInterval,
            () -> executorNodeTask.set(null)
        );
    }

    void startStopTask(ClusterChangedEvent event) {
        // Wait until cluster has recovered. Plus, start the task only when every node in the cluster supports IX
        if (event.state().clusterRecovered() == false
            || featureService.clusterHasFeature(event.state(), MeteringFeatures.INDEX_INFO_SUPPORTED) == false) {
            return;
        }

        DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        if (masterNode == null) {
            // no master yet
            return;
        }

        doStartStopTask(event.state());
    }

    private void doStartStopTask(ClusterState clusterState) {
        boolean indexSizeTaskRunningInCluster = SampledClusterMetricsSchedulingTask.findTask(clusterState) != null;

        boolean isElectedMaster = clusterService.state().nodes().isLocalNodeElectedMaster();
        // we should only start/stop task from single node, master is the best as it will go through it anyway
        if (isElectedMaster) {
            if (indexSizeTaskRunningInCluster == false && enabled) {
                startTask();
            }
            if (indexSizeTaskRunningInCluster && enabled == false) {
                stopTask();
            }
        }
    }

    private void startTask() {
        persistentTasksService.sendStartRequest(
            SampledClusterMetricsSchedulingTask.TASK_NAME,
            SampledClusterMetricsSchedulingTask.TASK_NAME,
            SampledClusterMetricsSchedulingTaskParams.INSTANCE,
            null,
            ActionListener.wrap(r -> logger.debug("Created MeteringIndexInfoTask task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("Failed to create MeteringIndexInfoTask task", e);
                }
            })
        );
    }

    private void stopTask() {
        persistentTasksService.sendRemoveRequest(
            SampledClusterMetricsSchedulingTask.TASK_NAME,
            null,
            ActionListener.wrap(r -> logger.debug("Stopped MeteringIndexInfoTask task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.error("failed to remove MeteringIndexInfoTask task", e);
                }
            })
        );
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        doStartStopTask(clusterService.state());
    }

    private void updatePollInterval(TimeValue pollInterval) {
        if (Objects.equals(this.pollInterval, pollInterval) == false) {
            this.pollInterval = pollInterval;
            var task = executorNodeTask.get();
            if (task != null) {
                task.requestReschedule();
            }
        }
    }

    void shuttingDown(ClusterChangedEvent event) {
        DiscoveryNode node = clusterService.localNode();
        if (isNodeShuttingDown(event, node.getId())) {
            var persistentTask = SampledClusterMetricsSchedulingTask.findTask(event.state());
            if (persistentTask != null && persistentTask.isAssigned()) {
                if (node.getId().equals(persistentTask.getExecutorNode())) {
                    stopTask();
                }
            }
        }
    }

    private static boolean isNodeShuttingDown(ClusterChangedEvent event, String nodeId) {
        return event.previousState().metadata().nodeShutdowns().contains(nodeId) == false
            && event.state().metadata().nodeShutdowns().contains(nodeId);
    }
}
