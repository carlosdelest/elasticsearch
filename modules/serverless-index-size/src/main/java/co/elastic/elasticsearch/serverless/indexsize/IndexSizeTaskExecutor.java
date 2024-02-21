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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Persistent task executor that is managing the {@link IndexSizeTask}.
 */
public final class IndexSizeTaskExecutor extends PersistentTasksExecutor<IndexSizeTaskParams> {

    private static final Logger logger = LogManager.getLogger(IndexSizeTaskExecutor.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "index-size.task.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "index-size.task.poll.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Client client;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final ThreadPool threadPool;
    private final IndexSizeService indexSizeService;

    // Holds a reference to IndexSizeTask. This will have a valid value only on the executor node, otherwise it will be null.
    private final AtomicReference<IndexSizeTask> executorNodeTask = new AtomicReference<>();
    private final PersistentTasksService persistentTasksService;
    private volatile boolean enabled;
    private volatile TimeValue pollInterval;

    private IndexSizeTaskExecutor(
        Client client,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        ThreadPool threadPool,
        IndexSizeService indexSizeService,
        Settings settings
    ) {
        super(IndexSizeTask.TASK_NAME, threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.client = client;

        this.clusterService = clusterService;
        this.featureService = featureService;
        this.threadPool = threadPool;
        this.indexSizeService = indexSizeService;
        this.persistentTasksService = persistentTasksService;
        this.enabled = ENABLED_SETTING.get(settings);
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
    }

    private static void registerListeners(ClusterService clusterService, ClusterSettings clusterSettings, IndexSizeTaskExecutor executor) {
        clusterService.addListener(executor::startStopTask);
        clusterService.addListener(executor::shuttingDown);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, executor::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, executor::updatePollInterval);
    }

    public static IndexSizeTaskExecutor create(
        Client client,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        ThreadPool threadPool,
        IndexSizeService indexSizeService,
        Settings settings
    ) {
        var executor = new IndexSizeTaskExecutor(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            indexSizeService,
            settings
        );
        registerListeners(clusterService, clusterService.getClusterSettings(), executor);
        return executor;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, IndexSizeTaskParams params, PersistentTaskState state) {
        IndexSizeTask indexSizeTask = (IndexSizeTask) task;
        executorNodeTask.set(indexSizeTask);
        DiscoveryNode node = clusterService.localNode();
        logger.info("Node [{{}}{{}}] is selected as the current Index Size node.", node.getName(), node.getId());
        if (this.enabled) {
            indexSizeTask.run();
        }
    }

    @Override
    protected IndexSizeTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<IndexSizeTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        logger.debug("Creating IndexSizeTask [{}][{}]", type, action);
        return new IndexSizeTask(
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            threadPool,
            indexSizeService,
            client,
            () -> pollInterval,
            () -> executorNodeTask.set(null)
        );
    }

    void startStopTask(ClusterChangedEvent event) {
        // Wait until cluster has recovered. Plus, start the task only when every node in the cluster supports IX
        if (event.state().clusterRecovered() == false
            || featureService.clusterHasFeature(event.state(), IndexSizePlugin.INDEX_SIZE_SUPPORTED) == false) {
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
        boolean indexSizeTaskRunningInCluster = IndexSizeTask.findTask(clusterState) != null;

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
            IndexSizeTask.TASK_NAME,
            IndexSizeTask.TASK_NAME,
            IndexSizeTaskParams.INSTANCE,
            ActionListener.wrap(r -> logger.debug("Created the IX task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("Failed to create the IX task", e);
                }
            })
        );
    }

    private void stopTask() {
        persistentTasksService.sendRemoveRequest(IndexSizeTask.TASK_NAME, ActionListener.wrap(r -> logger.debug("Stopped IX task"), e -> {
            Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
            if (t instanceof ResourceNotFoundException == false) {
                logger.error("failed to remove IX task", e);
            }
        }));
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
            stopTask();
        }
    }

    private static boolean isNodeShuttingDown(ClusterChangedEvent event, String nodeId) {
        return event.previousState().metadata().nodeShutdowns().contains(nodeId) == false
            && event.state().metadata().nodeShutdowns().contains(nodeId);
    }
}
