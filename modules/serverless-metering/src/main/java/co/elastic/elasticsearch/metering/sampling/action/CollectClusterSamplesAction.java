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
import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
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

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_BROADCAST_ACTIVITY)) {
                this.searchActivity = Activity.readFrom(in);
                this.indexActivity = Activity.readFrom(in);
            } else {
                this.searchActivity = Activity.EMPTY;
                this.indexActivity = Activity.EMPTY;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_BROADCAST_ACTIVITY)) {
                searchActivity.writeTo(out);
                indexActivity.writeTo(out);
            }
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
        private final List<Exception> exceptions;

        public Response(StreamInput in) throws IOException {
            this.searchTierMemorySize = in.readVLong();
            this.indexTierMemorySize = in.readVLong();
            searchActivity = Activity.readFrom(in);
            indexActivity = Activity.readFrom(in);
            this.shardInfos = in.readImmutableMap(ShardId::new, ShardInfoMetrics::from);
            this.exceptions = in.readCollectionAsImmutableList(StreamInput::readException);
        }

        public Response(
            long searchTierMemorySize,
            long indexTierMemorySize,
            final Activity searchActivity,
            final Activity indexActivity,
            Map<ShardId, ShardInfoMetrics> shardInfos,
            List<Exception> exceptions
        ) {
            this.searchTierMemorySize = searchTierMemorySize;
            this.indexTierMemorySize = indexTierMemorySize;
            this.searchActivity = searchActivity;
            this.indexActivity = indexActivity;
            this.shardInfos = shardInfos;
            this.exceptions = exceptions;
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeVLong(searchTierMemorySize);
            output.writeVLong(indexTierMemorySize);
            searchActivity.writeTo(output);
            indexActivity.writeTo(output);
            output.writeMap(shardInfos, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
            output.writeGenericList(exceptions, StreamOutput::writeException);
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
                    && Objects.equals(exceptions, response.exceptions);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchTierMemorySize, indexTierMemorySize, searchActivity, indexActivity, shardInfos, exceptions);
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

        public boolean isComplete() {
            return exceptions.isEmpty();
        }

        public List<Exception> getFailures() {
            return exceptions;
        }
    }
}
