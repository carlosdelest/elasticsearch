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

package co.elastic.elasticsearch.ml.serverless.actionfilters;

import co.elastic.elasticsearch.ml.serverless.ServerlessMachineLearningExtension;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response.TrainedModelStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;

import java.time.Instant;
import java.util.List;

public class GetTrainedModelsStatsResponseFilter extends ApiFilteringActionFilter<GetTrainedModelsStatsAction.Response> {

    public GetTrainedModelsStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetTrainedModelsStatsAction.NAME, GetTrainedModelsStatsAction.Response.class);
    }

    /**
     * This method rolls up the trained model deployment stats to appear to be from a single virtual "serverless" node.
     */
    @Override
    protected GetTrainedModelsStatsAction.Response filterResponse(GetTrainedModelsStatsAction.Response response) {
        QueryPage<TrainedModelStats> page = response.getResources();
        if (page.count() == 0) {
            return response;
        } else {
            return new GetTrainedModelsStatsAction.Response(
                new QueryPage<>(
                    page.results().stream().map(GetTrainedModelsStatsResponseFilter::rollupDeploymentStats).toList(),
                    page.count(),
                    page.getResultsField()
                )
            );
        }
    }

    static TrainedModelStats rollupDeploymentStats(TrainedModelStats stats) {
        if (stats.getDeploymentStats() == null) {
            return stats;
        } else {
            AssignmentStats rolledUpAssignmentStats = rollupAssignmentStats(stats.getDeploymentStats());
            return new TrainedModelStats(
                stats.getModelId(),
                stats.getModelSizeStats(),
                stats.getIngestStats(),
                stats.getPipelineCount(),
                stats.getInferenceStats(),
                rolledUpAssignmentStats
            );
        }
    }

    static AssignmentStats rollupAssignmentStats(AssignmentStats stats) {
        List<AssignmentStats.NodeStats> nodeStatsList = stats.getNodeStats();
        if (nodeStatsList == null || nodeStatsList.isEmpty()) {
            return stats;
        } else {
            AssignmentStats.NodeStats rolledUpNodeStats = rollupNodeStats(nodeStatsList);
            return new AssignmentStats(
                stats.getDeploymentId(),
                stats.getModelId(),
                stats.getThreadsPerAllocation(),
                stats.getNumberOfAllocations(),
                stats.getAdaptiveAllocationsSettings(),
                stats.getQueueCapacity(),
                stats.getCacheSize(),
                stats.getStartTime(),
                List.of(rolledUpNodeStats),
                stats.getPriority()
            );
        }
    }

    static AssignmentStats.NodeStats rollupNodeStats(List<AssignmentStats.NodeStats> nodeStatsList) {
        Long inferenceCount = null;
        Double totalInferenceTime = null;
        Double totalInferenceTimeExcludingCacheHit = null;
        Instant lastAccess = null;
        Integer pendingCount = null;
        int errorCount = 0;
        Long cacheHitCount = null;
        int rejectedExecutionCount = 0;
        int timeoutCount = 0;
        RoutingStateAndReason routingState = null;
        Instant startTime = null;
        Integer threadsPerAllocation = null;
        Integer numberOfAllocations = null;
        long peakThroughput = 0;
        long throughputLastPeriod = 0;
        Double totalInferenceTimeLastPeriod = null;
        Long cacheHitCountLastPeriod = null;
        for (AssignmentStats.NodeStats nodesStats : nodeStatsList) {
            if (nodesStats.getInferenceCount().isPresent()) {
                long current = (inferenceCount == null) ? 0 : inferenceCount;
                inferenceCount = current + nodesStats.getInferenceCount().get();
                if (nodesStats.getAvgInferenceTime().isPresent()) {
                    double currentTotal = (totalInferenceTime == null) ? 0.0 : totalInferenceTime;
                    totalInferenceTime = currentTotal + nodesStats.getAvgInferenceTime().get() * nodesStats.getInferenceCount().get();
                }
            }
            if (nodesStats.getLastAccess() != null) {
                if (lastAccess == null || lastAccess.isBefore(nodesStats.getLastAccess())) {
                    lastAccess = nodesStats.getLastAccess();
                }
            }
            if (nodesStats.getPendingCount() != null) {
                int current = (pendingCount == null) ? 0 : pendingCount;
                pendingCount = current + nodesStats.getPendingCount();
            }
            errorCount += nodesStats.getErrorCount();
            if (nodesStats.getCacheHitCount().isPresent()) {
                long current = (cacheHitCount == null) ? 0 : cacheHitCount;
                cacheHitCount = current + nodesStats.getCacheHitCount().get();
                if (nodesStats.getAvgInferenceTimeExcludingCacheHit().isPresent() && nodesStats.getInferenceCount().isPresent()) {
                    double currentTotal = (totalInferenceTimeExcludingCacheHit == null) ? 0.0 : totalInferenceTimeExcludingCacheHit;
                    totalInferenceTimeExcludingCacheHit = currentTotal + nodesStats.getAvgInferenceTimeExcludingCacheHit().get()
                        * (nodesStats.getInferenceCount().get() - nodesStats.getCacheHitCount().get());
                }
            }
            if (nodesStats.getRoutingState() != null) {
                if (routingState == null) {
                    routingState = nodesStats.getRoutingState();
                } else {
                    routingState = combineRoutingStatesAndReasons(routingState, nodesStats.getRoutingState());
                }
            }
            if (nodesStats.getStartTime() != null) {
                if (startTime == null || startTime.isAfter(nodesStats.getStartTime())) {
                    startTime = nodesStats.getStartTime();
                }
            }
            if (nodesStats.getThreadsPerAllocation() != null) {
                int current = (threadsPerAllocation == null) ? 0 : threadsPerAllocation;
                threadsPerAllocation = Math.max(current, nodesStats.getThreadsPerAllocation());
            }
            if (nodesStats.getNumberOfAllocations() != null) {
                int current = (numberOfAllocations == null) ? 0 : numberOfAllocations;
                numberOfAllocations = current + nodesStats.getNumberOfAllocations();
            }
            rejectedExecutionCount += nodesStats.getRejectedExecutionCount();
            timeoutCount += nodesStats.getTimeoutCount();
            peakThroughput += nodesStats.getPeakThroughput();
            throughputLastPeriod += nodesStats.getThroughputLastPeriod();
            if (nodesStats.getAvgInferenceTimeLastPeriod() != null && nodesStats.getThroughputLastPeriod() > 0) {
                double current = (totalInferenceTimeLastPeriod == null) ? 0.0 : totalInferenceTimeLastPeriod;
                totalInferenceTimeLastPeriod = current + nodesStats.getAvgInferenceTimeLastPeriod() * nodesStats.getThroughputLastPeriod();
            }
            if (nodesStats.getCacheHitCountLastPeriod().isPresent()) {
                long current = (cacheHitCountLastPeriod == null) ? 0 : cacheHitCountLastPeriod;
                cacheHitCountLastPeriod = current + nodesStats.getCacheHitCountLastPeriod().get();
            }
        }
        return new AssignmentStats.NodeStats(
            ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE,
            inferenceCount,
            (totalInferenceTime == null) ? null : (totalInferenceTime / inferenceCount),
            (totalInferenceTimeExcludingCacheHit == null) ? null : (totalInferenceTimeExcludingCacheHit / (inferenceCount - cacheHitCount)),
            lastAccess,
            pendingCount,
            errorCount,
            cacheHitCount,
            rejectedExecutionCount,
            timeoutCount,
            routingState,
            startTime,
            threadsPerAllocation,
            numberOfAllocations,
            peakThroughput,
            throughputLastPeriod,
            (totalInferenceTimeLastPeriod == null) ? null : (totalInferenceTimeLastPeriod / throughputLastPeriod),
            cacheHitCountLastPeriod
        );
    }

    /**
     * Combine two routing states in a way that will confer some information while disguising the fact that
     * there might be multiple servers. We don't want to admit to the existence of servers in serverless.
     * The order of preference is STARTED, STARTING, STOPPING, FAILED, STOPPED.
     * @param x The first routing state, not <code>null</code>.
     * @param y The second routing state, not <code>null</code>.
     * @return The chosen routing state.
     */
    static RoutingStateAndReason combineRoutingStatesAndReasons(RoutingStateAndReason x, RoutingStateAndReason y) {
        assert x != null : "first argument was null";
        assert y != null : "second argument was null";
        if (x.getState() == RoutingState.STARTED) {
            return x;
        }
        if (y.getState() == RoutingState.STARTED) {
            return y;
        }
        if (x.getState() == RoutingState.STARTING) {
            return x;
        }
        if (y.getState() == RoutingState.STARTING) {
            return y;
        }
        if (x.getState() == RoutingState.STOPPING) {
            return x;
        }
        if (y.getState() == RoutingState.STOPPING) {
            return y;
        }
        if (x.getState() == RoutingState.FAILED) {
            return x;
        }
        if (y.getState() == RoutingState.FAILED) {
            return y;
        }
        return x;
    }
}
