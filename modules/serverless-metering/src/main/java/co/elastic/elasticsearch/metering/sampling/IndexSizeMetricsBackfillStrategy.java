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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.UsageMetadata;
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.metering.sampling.IndexSizeMetricsProvider.updateIxUsageMetadata;

public class IndexSizeMetricsBackfillStrategy implements SampledMetricsProvider.BackfillStrategy {
    private final Activity searchActivity;
    private final Duration activityCoolDownPeriod;

    protected IndexSizeMetricsBackfillStrategy(Activity searchActivity, Duration activityCoolDownPeriod) {
        this.searchActivity = searchActivity;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
    }

    @Override
    public void constant(MetricValue sample, Instant timestamp, SampledMetricsProvider.BackfillSink sink) {
        if (sample.meteredObjectCreationTime() == null || timestamp.isBefore(sample.meteredObjectCreationTime()) == false) {
            Map<String, String> usageMetadata = new HashMap<>(sample.usageMetadata());
            updateIxUsageMetadata(usageMetadata, null, searchActivity.activitySnapshot(timestamp, activityCoolDownPeriod));
            sink.add(sample, sample.value(), usageMetadata, timestamp);
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
        long interpolatedValue = DefaultSampledMetricsBackfillStrategy.interpolateValueForTimestamp(
            currentTimestamp,
            currentSample.value(),
            previousTimestamp,
            previousSample.value(),
            backfillTimestamp
        );
        Map<String, String> usageMetadata = new HashMap<>(currentSample.usageMetadata());
        var spMinInfo = SPMinProvisionedMemoryCalculator.SPMinInfo.minOf(currentSample, previousSample);
        var previousActivity = getLastActivityTime(previousSample);
        var snapshot = searchActivity.activitySnapshot(backfillTimestamp, previousActivity, activityCoolDownPeriod);
        updateIxUsageMetadata(usageMetadata, spMinInfo, snapshot);
        sink.add(currentSample, interpolatedValue, usageMetadata, backfillTimestamp);
    }

    private static Instant getLastActivityTime(MetricValue metricValue) {
        String lastActivityTime = metricValue.usageMetadata().get(UsageMetadata.SEARCH_TIER_LATEST_ACTIVITY_TIMESTAMP);
        return lastActivityTime == null ? null : Instant.parse(lastActivityTime);
    }
}
