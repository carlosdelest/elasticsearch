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

package co.elastic.elasticsearch.metering.action;

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
 * Action used to collect shard metering info from all data node and reduce it to a single response for {@link MeteringIndexInfoService}.
 */
public class CollectMeteringShardInfoAction {

    public static final String NAME = "cluster:monitor/collect/metering/shard-info";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

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
            return "Collect shard metering information from all search nodes";
        }
    }

    public static class Response extends ActionResponse {
        private final Map<ShardId, MeteringShardInfo> shardInfo;
        private final List<Exception> exceptions;

        public Response(StreamInput in) throws IOException {
            this.shardInfo = in.readImmutableMap(ShardId::new, MeteringShardInfo::from);
            this.exceptions = in.readCollectionAsImmutableList(StreamInput::readException);
        }

        public Response(Map<ShardId, MeteringShardInfo> shardInfo, List<Exception> exceptions) {
            this.shardInfo = shardInfo;
            this.exceptions = exceptions;
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeMap(shardInfo, (out, value) -> value.writeTo(out), (out, value) -> value.writeTo(out));
            output.writeGenericList(exceptions, StreamOutput::writeException);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return Objects.equals(shardInfo, response.shardInfo) && Objects.equals(exceptions, response.exceptions);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardInfo, exceptions);
        }

        public Map<ShardId, MeteringShardInfo> getShardInfo() {
            return shardInfo;
        }

        public boolean isComplete() {
            return exceptions.isEmpty();
        }

        public List<Exception> getFailures() {
            return exceptions;
        }
    }
}
