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

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
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

public class MeteringIndexInfoTask extends AllocatedPersistentTask {

    public static final String TASK_NAME = "metering-index-info";

    private static final Logger logger = LogManager.getLogger(MeteringIndexInfoTask.class);
    private final ThreadPool threadPool;
    volatile Scheduler.ScheduledCancellable scheduled;
    private final Client client;
    private final Supplier<TimeValue> pollIntervalSupplier;
    private final Runnable notifyCancelled;

    private final MeteringIndexInfoService meteringIndexInfoService;

    MeteringIndexInfoTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        ThreadPool threadPool,
        MeteringIndexInfoService meteringIndexInfoService,
        Client client,
        Supplier<TimeValue> pollIntervalSupplier,
        Runnable notifyCancelled
    ) {
        super(id, type, action, description, parentTask, headers);
        this.threadPool = threadPool;
        this.meteringIndexInfoService = meteringIndexInfoService;
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

    @Nullable
    static PersistentTasksCustomMetadata.PersistentTask<?> findTask(ClusterState clusterState) {
        PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (taskMetadata == null) {
            return null;
        }
        return taskMetadata.getTask(TASK_NAME);
    }

    void run() {
        if (isCancelled() || isCompleted()) {
            return;
        }
        try {
            meteringIndexInfoService.updateMeteringShardInfo(client);
        } catch (Exception e) {
            logger.error("Failed during MeteringIndexInfoTask run", e);
        }
        scheduleNextRun(pollIntervalSupplier.get());
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
