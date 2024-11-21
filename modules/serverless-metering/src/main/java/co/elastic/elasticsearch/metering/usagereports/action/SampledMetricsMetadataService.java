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

package co.elastic.elasticsearch.metering.usagereports.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

class SampledMetricsMetadataService {
    private static final Logger logger = LogManager.getLogger(SampledMetricsMetadataService.class);

    private final MasterServiceTaskQueue<UpsertSampledMetricsMetadataTask> taskQueue;

    SampledMetricsMetadataService(ClusterService clusterService) {
        this.taskQueue = clusterService.createTaskQueue("index info collector metadata", Priority.NORMAL, new Executor());
    }

    void update(SampledMetricsMetadata metadata, TimeValue timeout, ActionListener<Void> listener) {
        logger.debug("Updating metadata to {}", metadata);
        // Write metadata with new committed timestamp into cluster state and only complete once successfully updated cluster state.
        // This is to apply backpressure to avoid running a reporting iteration with an outdated sample timestamp.
        // Publish failures, e.g. NotMasterException will be retried by TransportMasterNodeAction
        taskQueue.submitTask("metering-sampled-metrics-metadata-update", new UpsertSampledMetricsMetadataTask(metadata, listener), timeout);
    }

    // visible for testing
    record UpsertSampledMetricsMetadataTask(SampledMetricsMetadata metadata, ActionListener<Void> listener)
        implements
            ClusterStateTaskListener {
        private static final String FAILURE_MESSAGE = "failure during sampled metrics metadata update";

        @Override
        public void onFailure(@Nullable Exception e) {
            if (MasterService.isPublishFailureException(e) == false) {
                logger.warn(FAILURE_MESSAGE, e); // otherwise retried by TransportMasterNodeAction
            }
            listener.onFailure(e);
        }
    }

    // visible for testing
    static class Executor extends SimpleBatchedExecutor<UpsertSampledMetricsMetadataTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(UpsertSampledMetricsMetadataTask task, ClusterState clusterState) {
            return Tuple.tuple(
                task.metadata.equals(SampledMetricsMetadata.getFromClusterState(clusterState))
                    ? clusterState
                    : clusterState.copyAndUpdate(b -> b.putCustom(SampledMetricsMetadata.TYPE, task.metadata)),
                null
            );
        }

        @Override
        public void taskSucceeded(UpsertSampledMetricsMetadataTask task, Void unused) {
            logger.debug("Successfully updated metadata to {}", task.metadata);
            task.listener.onResponse(null);
        }
    }
}
