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

package co.elastic.elasticsearch.serverless.autoscaling.model;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;

import static co.elastic.elasticsearch.serverless.autoscaling.model.MetricsTests.randomMetric;

public class TierMetricsTests extends AbstractWireSerializingTestCase<TierMetrics> {

    public static TierMetrics randomTierMetrics() {
        return new TierMetrics(randomMap(1, 20, () -> Tuple.tuple(randomAlphaOfLength(4), randomMetric())));
    }

    public static TierMetrics mutateTierMetrics(TierMetrics instance) {
        HashMap<String, Metric> mutatedMetrics = new HashMap<>(instance.metrics());
        mutatedMetrics.put(randomAlphaOfLength(5), randomMetric());
        return new TierMetrics(mutatedMetrics);
    }

    @Override
    protected Writeable.Reader<TierMetrics> instanceReader() {
        return TierMetrics::new;
    }

    @Override
    protected TierMetrics createTestInstance() {
        return randomTierMetrics();
    }

    @Override
    protected TierMetrics mutateInstance(TierMetrics instance) throws IOException {
        return mutateTierMetrics(instance);
    }
}
