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

package co.elastic.elasticsearch.metering.sampling.action;

import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Objects;

/**
 * Action used to collect metering samples from nodes and reduce those to a single response for {@link SampledClusterMetricsService}.
 */
public class CollectClusterSamplesAction {

    public static final String NAME = "cluster:monitor/collect/metering/samples";

    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    public static class Request extends ActionRequest {
        private final Activity searchActivity;
        private final Activity indexActivity;

        public Request(Activity searchActivity, Activity indexActivity) {
            this.searchActivity = searchActivity;
            this.indexActivity = indexActivity;
        }

        public void writeTo(StreamOutput output) {
            TransportAction.localOnly();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Collect samples from nodes";
        }

        public Activity getSearchActivity() {
            return searchActivity;
        }

        public Activity getIndexActivity() {
            return indexActivity;
        }
    }

    public static class Response extends ActionResponse {
        private final long searchTierMemorySize;
        private final long indexTierMemorySize;
        private final Activity searchActivity;
        private final Activity indexActivity;
        private final Map<ShardId, ShardInfoMetrics> shardInfos;
        private final int searchNodes;
        private final int searchNodeErrors;
        private final int indexNodes;
        private final int indexNodeErrors;

        public Response(
            long searchTierMemorySize,
            long indexTierMemorySize,
            final Activity searchActivity,
            final Activity indexActivity,
            Map<ShardId, ShardInfoMetrics> shardInfos,
            int searchNodes,
            int searchNodeErrors,
            int indexNodes,
            int indexNodeErrors
        ) {
            this.searchTierMemorySize = searchTierMemorySize;
            this.indexTierMemorySize = indexTierMemorySize;
            this.searchActivity = searchActivity;
            this.indexActivity = indexActivity;
            this.shardInfos = shardInfos;
            this.searchNodes = searchNodes;
            this.searchNodeErrors = searchNodeErrors;
            this.indexNodes = indexNodes;
            this.indexNodeErrors = indexNodeErrors;
            assert searchNodes >= searchNodeErrors && searchNodeErrors >= 0;
            assert indexNodes >= indexNodeErrors && indexNodeErrors >= 0;
        }

        @Override
        public void writeTo(StreamOutput output) {
            TransportAction.localOnly();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return searchTierMemorySize == response.searchTierMemorySize
                    && indexTierMemorySize == response.indexTierMemorySize
                    && Objects.equals(searchActivity, response.searchActivity)
                    && Objects.equals(indexActivity, response.indexActivity)
                    && Objects.equals(shardInfos, response.shardInfos)
                    && searchNodes == response.searchNodes
                    && searchNodeErrors == response.searchNodeErrors
                    && indexNodes == response.indexNodes
                    && indexNodeErrors == response.indexNodeErrors;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                searchTierMemorySize,
                indexTierMemorySize,
                searchActivity,
                indexActivity,
                shardInfos,
                searchNodes,
                searchNodeErrors,
                indexNodes,
                indexNodeErrors
            );
        }

        public long getExtrapolatedSearchTierMemorySize() {
            if (searchNodes <= searchNodeErrors) {
                return 0; // no memory metrics available
            } else if (searchNodeErrors == 0) {
                return searchTierMemorySize; // no extrapolation needed
            }
            return (searchTierMemorySize / (searchNodes - searchNodeErrors)) * searchNodes;
        }

        public long getExtrapolatedIndexTierMemorySize() {
            if (indexNodes <= indexNodeErrors) {
                return 0; // no memory metrics available
            } else if (indexNodeErrors == 0) {
                return indexTierMemorySize; // no extrapolation needed
            }
            return (indexTierMemorySize / (indexNodes - indexNodeErrors)) * indexNodes;
        }

        public int getSearchNodes() {
            return searchNodes;
        }

        public int getSearchNodeErrors() {
            return searchNodeErrors;
        }

        public int getIndexNodes() {
            return indexNodes;
        }

        public int getIndexNodeErrors() {
            return indexNodeErrors;
        }

        public long getIndexTierMemorySize() {
            return indexTierMemorySize;
        }

        public long getSearchTierMemorySize() {
            return searchTierMemorySize;
        }

        public Activity getSearchActivity() {
            return searchActivity;
        }

        public Activity getIndexActivity() {
            return indexActivity;
        }

        public Map<ShardId, ShardInfoMetrics> getShardInfos() {
            return shardInfos;
        }

        public boolean isPartialSuccess() {
            return searchNodeErrors > 0 || indexNodeErrors > 0;
        }
    }
}
