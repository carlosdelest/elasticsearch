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

import org.elasticsearch.telemetry.metric.LongCounter;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.updateUsageMetadata;
import static co.elastic.elasticsearch.metering.sampling.VCUSampledMetricsBackfillStrategy.DefaultReason.NOT_ENOUGH_PERIODS;
import static co.elastic.elasticsearch.metering.sampling.VCUSampledMetricsBackfillStrategy.DefaultReason.UNKNOWN_ACTIVITY;

public class VCUSampledMetricsBackfillStrategy implements SampledMetricsProvider.BackfillStrategy {
    private final Activity searchActivity;
    private final Activity indexActivity;
    private final Duration activityCoolDownPeriod;
    private final LongCounter defaultActivityReturnedCounter;

    enum DefaultReason {
        NOT_ENOUGH_PERIODS,
        UNKNOWN_ACTIVITY // empty or missing previous activity
    }

    enum BackfillType {
        CONSTANT,
        INTERPOLATED
    }

    public VCUSampledMetricsBackfillStrategy(
        Activity searchActivity,
        Activity indexActivity,
        Duration activityCoolDownPeriod,
        LongCounter defaultActivityReturnedCounter
    ) {
        this.searchActivity = searchActivity;
        this.indexActivity = indexActivity;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
        this.defaultActivityReturnedCounter = defaultActivityReturnedCounter;
    }

    @Override
    public void constant(MetricValue sample, Instant timestamp, SampledMetricsProvider.BackfillSink sink) {
        var tier = getUsageTier(sample);
        var snapshot = wasActive(tier, timestamp, null);
        if (snapshot == Activity.DEFAULT_NOT_ACTIVE) {
            trackFallbackToInactive(tier, BackfillType.CONSTANT, UNKNOWN_ACTIVITY);
        }
        var usageMetadata = new HashMap<>(sample.usageMetadata()); // reuse previous spMinInfo
        updateUsageMetadata(usageMetadata, snapshot, null, tier);
        sink.add(sample, sample.value(), usageMetadata, timestamp);
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
        var interpolatedVCU = Long.min(currentSample.value(), previousSample.value());
        var tier = getUsageTier(currentSample);

        var spMinInfo = SPMinProvisionedMemoryCalculator.SPMinInfo.minOf(currentSample, previousSample);
        var previousActivity = getLastActivityTime(previousSample);
        var snapshot = wasActive(tier, backfillTimestamp, previousActivity);
        if (snapshot == Activity.DEFAULT_NOT_ACTIVE) {
            var reason = getTierActivity(tier).isEmpty() || previousActivity == null ? UNKNOWN_ACTIVITY : NOT_ENOUGH_PERIODS;
            trackFallbackToInactive(tier, BackfillType.INTERPOLATED, reason);
        }
        var usageMetadata = new HashMap<String, String>();
        updateUsageMetadata(usageMetadata, snapshot, spMinInfo, tier);
        sink.add(currentSample, interpolatedVCU, usageMetadata, backfillTimestamp);
    }

    private Activity.Snapshot wasActive(String tier, Instant backfillTimestamp, Instant previousLastActivity) {
        var tierActivity = getTierActivity(tier);
        return tierActivity.activitySnapshot(backfillTimestamp, previousLastActivity, activityCoolDownPeriod);
    }

    private Activity getTierActivity(String tier) {
        return tier.equals("search") ? searchActivity : indexActivity;
    }

    private static String getUsageTier(MetricValue sample) {
        String tier = sample.usageMetadata().get(UsageMetadata.APPLICATION_TIER);
        assert tier == "search" || tier == "index";
        return tier;
    }

    private static Instant getLastActivityTime(MetricValue metricValue) {
        String lastActivityTime = metricValue.usageMetadata().get(UsageMetadata.LATEST_ACTIVITY_TIMESTAMP);
        return lastActivityTime == null ? null : Instant.parse(lastActivityTime);
    }

    private void trackFallbackToInactive(String tier, BackfillType backfillType, DefaultReason reason) {
        Map<String, Object> attributes = Map.of("tier", tier, "backfill-type", backfillType.name(), "reason", reason.name());
        defaultActivityReturnedCounter.incrementBy(1L, attributes);
    }
}
