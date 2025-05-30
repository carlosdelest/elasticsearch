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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.Supplier;

public class SampledClusterMetricsSchedulingTask extends AllocatedPersistentTask {
    // the role assignment of the persistent task, it won't run on any node that doesn't have this role
    public static final DiscoveryNodeRole ASSIGNED_ROLE = DiscoveryNodeRole.SEARCH_ROLE;
    public static final String TASK_NAME = "metering-index-info";

    private static final Logger logger = LogManager.getLogger(SampledClusterMetricsSchedulingTask.class);
    private final ThreadPool threadPool;
    volatile Scheduler.ScheduledCancellable scheduled;
    private final Client client;
    private final Supplier<TimeValue> pollIntervalSupplier;
    private final Runnable notifyCancelled;

    private final SampledClusterMetricsService clusterMetricsService;

    public static DiscoveryNode findTaskNode(ClusterState clusterState) {
        var taskNodeId = findTaskNodeId(clusterState);
        return taskNodeId != null ? clusterState.nodes().get(taskNodeId) : null;
    }

    public static String findTaskNodeId(ClusterState clusterState) {
        var task = findTask(clusterState);
        return task != null && task.isAssigned() ? task.getExecutorNode() : null;
    }

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> findTask(ClusterState clusterState) {
        PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        return taskMetadata != null ? taskMetadata.getTask(TASK_NAME) : null;
    }

    SampledClusterMetricsSchedulingTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        ThreadPool threadPool,
        SampledClusterMetricsService clusterMetricsService,
        Client client,
        Supplier<TimeValue> pollIntervalSupplier,
        Runnable notifyCancelled
    ) {
        super(id, type, action, description, parentTask, headers);
        this.threadPool = threadPool;
        this.clusterMetricsService = clusterMetricsService;
        this.client = client;
        this.pollIntervalSupplier = pollIntervalSupplier;
        this.notifyCancelled = notifyCancelled;
    }

    @Override
    protected void onCancelled() {
        if (scheduled != null) {
            scheduled.cancel();
        }
        markAsCompleted();
        notifyCancelled.run();
    }

    void run() {
        if (isCancelled() || isCompleted()) {
            return;
        }

        clusterMetricsService.updateSamples(client, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                scheduleNextRun(pollIntervalSupplier.get());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Sampling task run failed", e);
                scheduleNextRun(pollIntervalSupplier.get());
            }
        });
    }

    private void scheduleNextRun(TimeValue time) {
        if (threadPool.scheduler().isShutdown() == false) {
            scheduled = threadPool.schedule(this::run, time, threadPool.generic());
        }
    }

    /**
     * This method requests the task to be rescheduled and run immediately, presumably because a dynamic property supplied by
     * pollIntervalSupplier has changed. This method does nothing if this task is cancelled, completed, or has not yet been
     * scheduled to run for the first time. It cancels any existing scheduled run.
     */
    void requestReschedule() {
        if (isCancelled() || isCompleted() || scheduled == null) {
            return;
        }
        var cancelled = scheduled.cancel();
        if (cancelled) {
            scheduleNextRun(TimeValue.ZERO);
        }
    }
}
