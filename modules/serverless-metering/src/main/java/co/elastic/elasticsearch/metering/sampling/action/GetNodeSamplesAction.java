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
import java.util.Map;
import java.util.Objects;

/**
 * Action used to collect metering samples (including shard infos) from a single data node.
 */
public class GetNodeSamplesAction {

    public static final String NAME = "cluster:monitor/get/metering/samples";

    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    public static class Request extends ActionRequest {
        private final String cacheToken;
        private final Activity searchActivity;
        private final Activity indexActivity;

        /**
         * Creates a new request, specifying a token to use on the target node to check if the cached info (if any) matches
         * the requesting node, or if it is stale and should not be used and replaced.
         * @param cacheToken a token to be used to check if the cached info (if any) is valid or stale
         * @param searchActivity search activity to broadcast and merge with other node's activities
         * @param indexActivity index activity to broadcast and merge with other node's activities
         */
        public Request(String cacheToken, Activity searchActivity, Activity indexActivity) {
            this.cacheToken = cacheToken;
            this.searchActivity = searchActivity;
            this.indexActivity = indexActivity;
            assert cacheToken != null : "cacheToken required to get node samples";
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.cacheToken = in.readString();
            if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_BROADCAST_ACTIVITY)) {
                this.searchActivity = Activity.readFrom(in);
                this.indexActivity = Activity.readFrom(in);
            } else {
                this.searchActivity = Activity.EMPTY;
                this.indexActivity = Activity.EMPTY;
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Get shard metering information from a single data node";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(cacheToken);
            if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_BROADCAST_ACTIVITY)) {
                searchActivity.writeTo(out);
                indexActivity.writeTo(out);
            }
        }

        public String getCacheToken() {
            return cacheToken;
        }

        public Activity getSearchActivity() {
            return searchActivity;
        }

        public Activity getIndexActivity() {
            return indexActivity;
        }
    }

    public static class Response extends ActionResponse {
        private final long physicalMemorySize;
        private final Activity searchActivity;
        private final Activity indexActivity;
        private final Map<ShardId, ShardInfoMetrics> shardInfos;

        public Response(
            long physicalMemorySize,
            final Activity searchActivity,
            final Activity indexActivity,
            final Map<ShardId, ShardInfoMetrics> shardSizes
        ) {
            this.physicalMemorySize = physicalMemorySize;
            this.searchActivity = Objects.requireNonNull(searchActivity);
            this.indexActivity = Objects.requireNonNull(indexActivity);
            this.shardInfos = Objects.requireNonNull(shardSizes);
        }

        public Response(StreamInput in) throws IOException {
            this.physicalMemorySize = in.readVLong();
            searchActivity = Activity.readFrom(in);
            indexActivity = Activity.readFrom(in);
            this.shardInfos = in.readImmutableMap(ShardId::new, ShardInfoMetrics::from);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeVLong(physicalMemorySize);
            searchActivity.writeTo(output);
            indexActivity.writeTo(output);
            output.writeMap(shardInfos, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return physicalMemorySize == response.physicalMemorySize
                    && Objects.equals(searchActivity, response.searchActivity)
                    && Objects.equals(indexActivity, response.indexActivity)
                    && Objects.equals(shardInfos, response.shardInfos);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(physicalMemorySize, searchActivity, indexActivity, shardInfos);
        }

        public Map<ShardId, ShardInfoMetrics> getShardInfos() {
            return shardInfos;
        }

        public Activity getSearchActivity() {
            return searchActivity;
        }

        public Activity getIndexActivity() {
            return indexActivity;
        }

        public long getPhysicalMemorySize() {
            return physicalMemorySize;
        }
    }
}
