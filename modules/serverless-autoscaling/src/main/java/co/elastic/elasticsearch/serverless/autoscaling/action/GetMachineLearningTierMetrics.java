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

import co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetrics;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class GetMachineLearningTierMetrics {

    public static final String TIER_NAME = "ml";
    public static final String NAME = "cluster:internal/serverless/autoscaling/get_serverless_" + TIER_NAME + "_tier_metrics";
    public static final ActionType<GetMachineLearningTierMetrics.Response> INSTANCE = new ActionType<>(NAME);

    private GetMachineLearningTierMetrics() {/* no instances */}

    public static class Request extends AbstractTierMetricsRequest<Request> {
        Request(TimeValue timeout) {
            super(TIER_NAME, timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(TIER_NAME, in);
        }
    }

    public static class Response extends ActionResponse {
        private final MachineLearningTierMetrics metrics;

        public Response(MachineLearningTierMetrics metrics) {
            this.metrics = metrics;
        }

        public Response(StreamInput in) throws IOException {
            metrics = new MachineLearningTierMetrics(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            metrics.writeTo(out);
        }

        public MachineLearningTierMetrics getMetrics() {
            return metrics;
        }
    }
}
