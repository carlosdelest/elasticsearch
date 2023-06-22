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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.autoscaling.model.TierMetricsTests.mutateTierMetrics;
import static co.elastic.elasticsearch.serverless.autoscaling.model.TierMetricsTests.randomTierMetrics;

public class TierMetricsResponseTests extends AbstractWireSerializingTestCase<TierMetricsResponse> {

    public static TierMetricsResponse randomTierMetricsResponse() {
        return new TierMetricsResponse(randomAlphaOfLength(10), randomTierMetrics());
    }

    @Override
    protected Writeable.Reader<TierMetricsResponse> instanceReader() {
        return TierMetricsResponse::new;
    }

    @Override
    protected TierMetricsResponse createTestInstance() {
        return randomTierMetricsResponse();
    }

    @Override
    protected TierMetricsResponse mutateInstance(TierMetricsResponse instance) throws IOException {
        switch (between(0, 1)) {
            case 0:
                return new TierMetricsResponse(randomAlphaOfLength(12), instance.getTierMetrics());
            case 1:
                return new TierMetricsResponse(instance.getTierName(), mutateTierMetrics(instance.getTierMetrics()));
            default:
                throw new AssertionError("Illegal randomization branch");
        }
    }
}
