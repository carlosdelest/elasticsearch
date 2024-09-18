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

    public static final String LEGACY_NAME = "cluster:monitor/get/metering/shard-info";
    public static final String NAME = "cluster:monitor/get/metering/samples";

    public static final ActionType<Response> INSTANCE = new ActionType<>(LEGACY_NAME);

    public static class Request extends ActionRequest {
        private final String cacheToken;

        /**
         * Creates a new request, specifying a token to use on the target node to check if the cached info (if any) matches
         * the requesting node, or if it is stale and should not be used and replaced.
         * @param cacheToken a token to be used to check if the cached info (if any) is valid or stale
         */
        public Request(String cacheToken) {
            this.cacheToken = cacheToken;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.cacheToken = in.readString();
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
        }

        public String getCacheToken() {
            return cacheToken;
        }
    }

    public static class Response extends ActionResponse {
        private final long physicalMemorySize;
        private final Map<ShardId, ShardInfoMetrics> shardInfos;

        public Response(long physicalMemorySize, final Map<ShardId, ShardInfoMetrics> shardSizes) {
            this.physicalMemorySize = physicalMemorySize;
            this.shardInfos = Objects.requireNonNull(shardSizes);
        }

        public Response(StreamInput in) throws IOException {
            this.physicalMemorySize = in.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_SAMPLE_MEMORY)
                ? in.readVLong()
                : 0;
            this.shardInfos = in.readImmutableMap(ShardId::new, ShardInfoMetrics::from);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            if (output.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_SAMPLE_MEMORY)) {
                output.writeVLong(physicalMemorySize);
            }
            output.writeMap(shardInfos, (out, value) -> value.writeTo(out), (out, value) -> value.writeTo(out));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return physicalMemorySize == response.physicalMemorySize && Objects.equals(shardInfos, response.shardInfos);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(physicalMemorySize, shardInfos);
        }

        public Map<ShardId, ShardInfoMetrics> getShardInfos() {
            return shardInfos;
        }

        public long getPhysicalMemorySize() {
            return physicalMemorySize;
        }
    }
}
