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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.indexing.IndexTierMetrics;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class GetIndexTierMetrics {
    public static final String TIER_NAME = "index";
    public static final String NAME = "cluster:internal/serverless/autoscaling/get_serverless_" + TIER_NAME + "_tier_metrics";
    public static final ActionType<Response> INSTANCE = ActionType.localOnly(NAME);

    private GetIndexTierMetrics() {/* no instances */}

    public static class Request extends AbstractTierMetricsRequest<Request> {
        Request(TimeValue timeout) {
            super(TIER_NAME, timeout);
        }

        Request(final StreamInput input) throws IOException {
            super(TIER_NAME, input);
        }
    }

    public static class Response extends ActionResponse {
        private final IndexTierMetrics indexTierMetrics;

        public Response(IndexTierMetrics indexTierMetrics) {
            this.indexTierMetrics = indexTierMetrics;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            indexTierMetrics = new IndexTierMetrics(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexTierMetrics.writeTo(out);
        }

        public IndexTierMetrics getMetrics() {
            return indexTierMetrics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(indexTierMetrics, response.indexTierMetrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTierMetrics);
        }

        @Override
        public String toString() {
            return "Response{" + "indexTierMetrics=" + indexTierMetrics + '}';
        }
    }
}
