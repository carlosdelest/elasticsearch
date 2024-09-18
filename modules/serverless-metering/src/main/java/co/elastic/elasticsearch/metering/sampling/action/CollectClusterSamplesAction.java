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

    public static final String LEGACY_NAME = "cluster:monitor/collect/metering/shard-info";
    public static final String NAME = "cluster:monitor/collect/metering/samples";

    // TODO migrate this to the new name once fully deployed and remove the legacy name
    public static final ActionType<Response> INSTANCE = new ActionType<>(LEGACY_NAME);

    public static class Request extends ActionRequest {
        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Collect samples from nodes";
        }
    }

    public static class Response extends ActionResponse {
        private final long searchTierMemorySize;
        private final long indexTierMemorySize;
        private final Map<ShardId, ShardInfoMetrics> shardInfos;
        private final List<Exception> exceptions;

        public Response(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_SAMPLE_MEMORY)) {
                this.searchTierMemorySize = in.readVLong();
                this.indexTierMemorySize = in.readVLong();
            } else {
                this.searchTierMemorySize = 0;
                this.indexTierMemorySize = 0;
            }
            this.shardInfos = in.readImmutableMap(ShardId::new, ShardInfoMetrics::from);
            this.exceptions = in.readCollectionAsImmutableList(StreamInput::readException);
        }

        public Response(
            long searchTierMemorySize,
            long indexTierMemorySize,
            Map<ShardId, ShardInfoMetrics> shardInfos,
            List<Exception> exceptions
        ) {
            this.searchTierMemorySize = searchTierMemorySize;
            this.indexTierMemorySize = indexTierMemorySize;
            this.shardInfos = shardInfos;
            this.exceptions = exceptions;
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            if (output.getTransportVersion().onOrAfter(ServerlessTransportVersions.METERING_SAMPLE_MEMORY)) {
                output.writeVLong(searchTierMemorySize);
                output.writeVLong(indexTierMemorySize);
            }
            output.writeMap(shardInfos, (out, value) -> value.writeTo(out), (out, value) -> value.writeTo(out));
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
                    && Objects.equals(shardInfos, response.shardInfos)
                    && Objects.equals(exceptions, response.exceptions);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchTierMemorySize, indexTierMemorySize, shardInfos, exceptions);
        }

        public long getIndexTierMemorySize() {
            return indexTierMemorySize;
        }

        public long getSearchTierMemorySize() {
            return searchTierMemorySize;
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
