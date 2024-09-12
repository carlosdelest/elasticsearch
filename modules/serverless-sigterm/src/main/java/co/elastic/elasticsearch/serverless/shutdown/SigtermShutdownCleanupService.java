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
package co.elastic.elasticsearch.serverless.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.core.Strings.format;

/**
 * Cleans up expired {@link SingleNodeShutdownMetadata} of type {@link SingleNodeShutdownMetadata.Type#SIGTERM} from cluster state.
 */
public class SigtermShutdownCleanupService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SigtermShutdownCleanupService.class);
    private static final long GRACE_PERIOD_SAFETY_PERCENTAGE = 10;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final MasterServiceTaskQueue<CleanupSigtermShutdownTask> taskQueue;

    ConcurrentHashMap<String, Scheduler.ScheduledCancellable> cleanups = new ConcurrentHashMap<>();

    @SuppressWarnings("this-escape")
    public SigtermShutdownCleanupService(ClusterService clusterService) {
        this.threadPool = clusterService.threadPool();
        this.executor = threadPool.generic();
        this.taskQueue = clusterService.createTaskQueue(
            "shutdown-sigterm-cleaner",
            Priority.NORMAL,
            new RemoveSigtermShutdownTaskExecutor()
        );
        clusterService.addListener(this);
    }

    /**
     * Schedule a {@link SubmitCleanupSigtermShutdown} action when there is a sigterm {@link SingleNodeShutdownMetadata} in the cluster
     * state.  Actions are scheduled based on the started time of the shutdown and the grace period.
     * See {@link #computeDelay(long, long, long)} for details in the calculation.  The action is rescheduled
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            // Only do this if we're the current master node.
            return;
        }

        NodesShutdownMetadata eventShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);

        if (eventShutdownMetadata == null) {
            return;
        }

        // Only fetch the time if necessary
        long now = Long.MIN_VALUE;

        for (SingleNodeShutdownMetadata shutdown : eventShutdownMetadata.getAll().values()) {
            if (shutdown.getType() == SIGTERM) {
                String nodeId = shutdown.getNodeId();
                Scheduler.ScheduledCancellable cleanup = cleanups.get(shutdown.getNodeId());
                if (cleanup != null) {
                    if (cleanup.isCancelled()) {
                        cleanups.remove(nodeId);
                    } else {
                        continue;
                    }
                }

                if (now == Long.MIN_VALUE) {
                    now = threadPool.absoluteTimeInMillis();
                }

                cleanups.put(
                    nodeId,
                    threadPool.schedule(
                        new SubmitCleanupSigtermShutdown(taskQueue, nodeId, cleanups::remove),
                        computeDelay(now, shutdown.getStartedAtMillis(), shutdown.getGracePeriod().millis()),
                        executor
                    )
                );
            }
        }
    }

    /**
     * The amount of time to wait until the {@param grace} has expired, plus a little extra for safety,
     * {@link #GRACE_PERIOD_SAFETY_PERCENTAGE}.  {@param now} is the current time and {@param started} is
     * when the shutdown was first seen in cluster state.  All times in milliseconds.
     * If, due to clock skew, {@param started} is in the future, the elapsed time is clamped to zero.
     * Never returns a negative {#link TimeValue}.
     */
    static TimeValue computeDelay(long now, long started, long grace) {
        long elapsed = now - started;
        if (elapsed < 0) {
            elapsed = 0;
        }
        long delay = (grace + grace / GRACE_PERIOD_SAFETY_PERCENTAGE) - elapsed;
        if (delay <= 0) {
            return TimeValue.ZERO;
        }
        return new TimeValue(delay);
    }

    /**
     * Collection of state necessary to submit a {@link CleanupSigtermShutdownTask}.  Calls {@param remove} right before task submission.
     */
    record SubmitCleanupSigtermShutdown(
        MasterServiceTaskQueue<CleanupSigtermShutdownTask> taskQueue,
        String nodeId,
        Consumer<String> remove
    ) implements Runnable {
        SubmitCleanupSigtermShutdown {
            Objects.requireNonNull(taskQueue);
            Objects.requireNonNull(nodeId);
            Objects.requireNonNull(remove);
        }

        @Override
        public void run() {
            remove.accept(nodeId);
            taskQueue.submitTask("sigterm-grace-period-expired", new CleanupSigtermShutdownTask(nodeId), null);
        }
    }

    record CleanupSigtermShutdownTask(String nodeId) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> format("failed to cleanup sigterm shutdown metadata for node [%s]", nodeId), e);
        }
    }

    static class RemoveSigtermShutdownTaskExecutor implements ClusterStateTaskExecutor<CleanupSigtermShutdownTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<CleanupSigtermShutdownTask> batchExecutionContext) throws Exception {
            ClusterState state = cleanupSigtermShutdowns(
                batchExecutionContext.taskContexts()
                    .stream()
                    .map(TaskContext::getTask)
                    .map(CleanupSigtermShutdownTask::nodeId)
                    .collect(Collectors.toUnmodifiableSet()),
                batchExecutionContext.initialState()
            );
            batchExecutionContext.taskContexts().forEach(taskContext -> taskContext.success(() -> {}));
            return state;
        }

        /**
         * Remove the {@link SingleNodeShutdownMetadata} of type SIGTERM for all {@param nodeIds} that are no longer in the cluster.
         */
        static ClusterState cleanupSigtermShutdowns(Set<String> nodeIds, ClusterState initialState) {
            var shutdownMetadata = new HashMap<>(initialState.metadata().nodeShutdowns().getAll());

            boolean modified = false;
            for (String nodeId : nodeIds) {
                if (initialState.nodes().nodeExists(nodeId)) {
                    logger.warn(format("cannot remove sigterm shutdown for node [%s] that has not left the cluster", nodeId));
                } else {
                    SingleNodeShutdownMetadata singleShutdown = shutdownMetadata.remove(nodeId);
                    if (singleShutdown == null) {
                        // Could happen if, for example, we've received a cluster state update after task submission but before shutdown
                        // removal.
                        logger.trace(() -> format("sigterm shutdown already removed for node [%s]", nodeId));
                    } else if (singleShutdown.getType() != SIGTERM) {
                        logger.warn(
                            format(
                                "not removing unexpected shutdown type [%s] for node [%s], expected SIGTERM",
                                singleShutdown.getType(),
                                nodeId
                            )
                        );
                        // this is not the shutdown we are looking for
                        shutdownMetadata.put(nodeId, singleShutdown);
                    } else {
                        modified = true;
                    }
                }
            }

            if (modified == false) {
                return initialState;
            }

            return ClusterState.builder(initialState)
                .metadata(
                    Metadata.builder(initialState.metadata())
                        .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                        .build()
                )
                .build();
        }
    }
}
