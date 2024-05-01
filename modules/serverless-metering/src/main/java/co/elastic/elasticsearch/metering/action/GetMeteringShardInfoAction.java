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
import java.util.Map;
import java.util.Objects;

/**
 * Action used to collect metering shard info from a single data node.
 */
public class GetMeteringShardInfoAction {

    public static final String NAME = "cluster:monitor/get/metering/shard-info";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

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
        private final Map<ShardId, MeteringShardInfo> meteringShardInfoMap;

        public Response(final Map<ShardId, MeteringShardInfo> shardSizes) {
            this.meteringShardInfoMap = Objects.requireNonNull(shardSizes);
        }

        public Response(StreamInput in) throws IOException {
            this.meteringShardInfoMap = in.readImmutableMap(ShardId::new, MeteringShardInfo::from);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeMap(meteringShardInfoMap, (out, value) -> value.writeTo(out), (out, value) -> value.writeTo(out));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return Objects.equals(meteringShardInfoMap, response.meteringShardInfoMap);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(meteringShardInfoMap);
        }

        public Map<ShardId, MeteringShardInfo> getMeteringShardInfoMap() {
            return meteringShardInfoMap;
        }
    }
}
