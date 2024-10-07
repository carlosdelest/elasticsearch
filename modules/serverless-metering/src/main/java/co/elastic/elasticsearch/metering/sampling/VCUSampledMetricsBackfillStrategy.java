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

import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.core.Nullable;

import java.time.Duration;
import java.time.Instant;

import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.buildUsageMetadata;

public class VCUSampledMetricsBackfillStrategy implements SampledMetricsProvider.BackfillStrategy {
    private final Activity searchActivity;
    private final Activity indexActivity;
    private final Duration activityCoolDownPeriod;

    public VCUSampledMetricsBackfillStrategy(Activity searchActivity, Activity indexActivity, Duration activityCoolDownPeriod) {
        this.searchActivity = searchActivity;
        this.indexActivity = indexActivity;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
    }

    @Override
    public void constant(MetricValue sample, Instant timestamp, SampledMetricsProvider.BackfillSink sink) {
        var spMinInfo = tryExtractSPMinInfo(sample);
        var tier = sample.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER);
        assert tier != null;
        var activity = tier.equals("search") ? searchActivity : indexActivity;
        var activityInfo = inferActivity(activity, null, timestamp, activityCoolDownPeriod);
        var usageMetadata = buildUsageMetadata(activityInfo.active(), activityInfo.lastActivityTime(), spMinInfo, tier);
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
        long interpolatedVCU = Long.min(currentSample.value(), previousSample.value());
        var spMinInfo = tryInterpolateSPMin(currentSample, previousSample);
        var tier = currentSample.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER);
        assert tier != null;
        var activity = tier.equals("search") ? searchActivity : indexActivity;
        Instant previousLastActivity = getPreviousLastActivityTime(previousSample);
        var activityInfo = inferActivity(activity, previousLastActivity, backfillTimestamp, activityCoolDownPeriod);
        var usageMetadata = buildUsageMetadata(activityInfo.active(), activityInfo.lastActivityTime(), spMinInfo, tier);

        sink.add(currentSample, interpolatedVCU, usageMetadata, backfillTimestamp);
    }

    @Nullable
    static SampledVCUMetricsProvider.SPMinInfo tryExtractSPMinInfo(MetricValue sample) {
        var spMinProvisioned = getSpMinProvisionedMemory(sample);
        if (spMinProvisioned != null) {
            var spMin = Long.parseLong(sample.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN));
            return new SampledVCUMetricsProvider.SPMinInfo(spMinProvisioned, spMin);
        }
        return null;
    }

    @Nullable
    static SampledVCUMetricsProvider.SPMinInfo tryInterpolateSPMin(MetricValue currentSample, MetricValue previousSample) {
        var currentSpMinProvisioned = getSpMinProvisionedMemory(currentSample);
        var previousSpMinProvisioned = getSpMinProvisionedMemory(previousSample);
        if (previousSpMinProvisioned != null && currentSpMinProvisioned != null) {
            var currentSpMin = getSpMin(currentSample);
            var previousSpMin = getSpMin(previousSample);
            // If sp_min_provisioned_memory is present sp_min is also present
            assert currentSpMin != null && previousSpMin != null;

            long spMinProvisioned = Long.min(previousSpMinProvisioned, currentSpMinProvisioned);
            long spMin = Long.min(previousSpMin, currentSpMin);
            return new SampledVCUMetricsProvider.SPMinInfo(spMinProvisioned, spMin);
        }
        return null;
    }

    static Activity.ActiveInfo inferActivity(
        Activity activity,
        @Nullable Instant previousLastActivity,
        Instant backfillTimestamp,
        Duration coolDown
    ) {
        assert previousLastActivity == null || backfillTimestamp.isBefore(previousLastActivity) == false;

        // First we check if backfillTimestamp is covered by current activity
        var currentActiveInfo = activity.wasActive(backfillTimestamp, coolDown);
        if (currentActiveInfo.isPresent()) {
            return currentActiveInfo.get();
        } else {
            // backfillTimestamp was not covered by current activity, there are a few reasons why this could have happened
            // 1) activity is empty because there have been no actions
            // 2) backfillTimestamp is before activity.firstActivityPreviousPeriod but after previousLastActivity + coolDown
            // 3) backfillTimestamp is before previousLastActivity + coolDown (and as always, after previousLastActivity)

            // In cases 1 and 2 we return the default value of no activity
            if (activity.isEmpty() || previousLastActivity == null || backfillTimestamp.isAfter(previousLastActivity.plus(coolDown))) {
                return Activity.DEFAULT_NOT_ACTIVE;
            }
            // This is case 3, we know there was activity and when it occurred
            return new Activity.ActiveInfo(true, previousLastActivity);
        }
    }

    private static Instant getPreviousLastActivityTime(MetricValue metricValue) {
        String lastActivityTime = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_LATEST_ACTIVITY_TIME);
        return lastActivityTime == null ? null : Instant.parse(lastActivityTime);
    }

    private static Long getSpMinProvisionedMemory(MetricValue metricValue) {
        String spMinProvisionedMem = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY);
        return spMinProvisionedMem == null ? null : Long.parseLong(spMinProvisionedMem);
    }

    private static Long getSpMin(MetricValue metricValue) {
        String spMin = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN);
        return spMin == null ? null : Long.parseLong(spMin);
    }
}
