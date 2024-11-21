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

package co.elastic.elasticsearch.metering.usagereports;

import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import java.time.Duration;
import java.time.Instant;

public class DefaultSampledMetricsBackfillStrategy implements SampledMetricsProvider.BackfillStrategy {
    public static final SampledMetricsProvider.BackfillStrategy INSTANCE = new DefaultSampledMetricsBackfillStrategy();

    protected DefaultSampledMetricsBackfillStrategy() {}

    @Override
    public void constant(MetricValue sample, Instant timestamp, SampledMetricsProvider.BackfillSink sink) {
        if (sample.meteredObjectCreationTime() == null || timestamp.isBefore(sample.meteredObjectCreationTime()) == false) {
            sink.add(sample, timestamp);
        }
    }

    @Override
    public void interpolate(
        Instant currentTimestamp,
        MetricValue currentSample,
        Instant previousTimestamp,
        MetricValue previousSample,
        Instant backfillTimestamp,
        SampledMetricsProvider.BackfillSink sink
    ) {
        long interpolatedValue = interpolateValueForTimestamp(
            currentTimestamp,
            currentSample.value(),
            previousTimestamp,
            previousSample.value(),
            backfillTimestamp
        );
        sink.add(currentSample, interpolatedValue, currentSample.usageMetadata(), backfillTimestamp);
    }

    static long interpolateValueForTimestamp(
        Instant currentTimestamp,
        long currentValue,
        Instant previousTimestamp,
        long previousValue,
        Instant pointTimestamp
    ) {
        assert previousTimestamp.isAfter(currentTimestamp) == false;
        assert pointTimestamp.isAfter(currentTimestamp) == false;
        assert pointTimestamp.isBefore(previousTimestamp) == false;

        if (currentValue == previousValue || currentTimestamp.equals(pointTimestamp)) {
            return currentValue;
        } else if (previousTimestamp.equals(pointTimestamp)) {
            return previousValue;
        }

        var timeSpan = Duration.between(currentTimestamp, previousTimestamp).toMillis();
        var valueSpan = currentValue - previousValue;
        var timeDelta = Duration.between(pointTimestamp, previousTimestamp).toMillis();
        if (timeDelta == 0) {
            return previousValue;
        }

        // Use an inverted ratio to avoid underflow/overflow/rounding errors
        var invertedRatio = (double) timeSpan / timeDelta;
        return previousValue + Math.round(valueSpan / invertedRatio);
    }
}
