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

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class GetMachineLearningTierMetrics extends ActionType<TierMetricsResponse> {

    public static final GetMachineLearningTierMetrics INSTANCE = new GetMachineLearningTierMetrics();
    public static final String TIER_NAME = "ml";
    public static final String NAME = "cluster:internal/serverless/autoscaling/get_serverless_" + TIER_NAME + "_tier_metrics";

    public GetMachineLearningTierMetrics() {
        super(NAME, in -> new TierMetricsResponse(in));
    }

    public static class Request extends TierMetricsRequest {
        Request(TimeValue timeout) {
            super(TIER_NAME, timeout);
        }

        Request(final StreamInput input) throws IOException {
            super(TIER_NAME, input);
        }
    }
}
