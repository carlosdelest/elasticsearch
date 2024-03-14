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

package co.elastic.elasticsearch.serverless.indexsize.action;

import co.elastic.elasticsearch.serverless.indexsize.MeteringShardInfo;

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
 * Action used to collect metering shard info from all data node and reduce it to a single Index Size (IX) information for IndexSizeService.
 */
public class GetShardInfoAction {

    public static final String NAME = "cluster:monitor/get/metering/shard-info";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    public static class Request extends ActionRequest {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Collect shard information from a search node";
        }
    }

    public static class Response extends ActionResponse {
        private final Map<ShardId, MeteringShardInfo> shardSizes;

        public Response(final Map<ShardId, MeteringShardInfo> shardSizes) {
            this.shardSizes = Objects.requireNonNull(shardSizes);
        }

        public Response(StreamInput in) throws IOException {
            this.shardSizes = in.readImmutableMap(ShardId::new, MeteringShardInfo::from);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeMap(shardSizes, (out, value) -> value.writeTo(out), (out, value) -> value.writeTo(out));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return Objects.equals(shardSizes, response.shardSizes);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardSizes);
        }

        public Map<ShardId, MeteringShardInfo> getShardSizes() {
            return shardSizes;
        }
    }
}
