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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.buildUsageMetadata;

public class VCUSampledMetricsBackfillStrategy implements SampledMetricsProvider.BackfillStrategy {

    private static final Logger logger = LogManager.getLogger(VCUSampledMetricsBackfillStrategy.class);
    private final Activity searchActivity;
    private final Activity indexActivity;
    private final Duration activityCoolDownPeriod;
    private final LongCounter defaultActivityReturnedCounter;

    enum DefaultReason {
        MISSING_PREVIOUS,
        NOT_ENOUGH_PERIODS,
        EMPTY_ACTIVITY;
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
        var spMinInfo = tryExtractSPMinInfo(sample);
        var tier = sample.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER);
        assert tier != null;
        var activity = tier.equals("search") ? searchActivity : indexActivity;
        var activityInfo = inferActivity(activity, null, timestamp, activityCoolDownPeriod, tier, BackfillType.CONSTANT);
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
        var spMinInfo = tryInterpolateSPMinInfo(currentSample, previousSample);
        var tier = currentSample.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER);
        assert tier != null;
        var activity = tier.equals("search") ? searchActivity : indexActivity;
        Instant previousLastActivity = getPreviousLastActivityTime(previousSample);
        var activityInfo = inferActivity(
            activity,
            previousLastActivity,
            backfillTimestamp,
            activityCoolDownPeriod,
            tier,
            BackfillType.INTERPOLATED
        );
        var usageMetadata = buildUsageMetadata(activityInfo.active(), activityInfo.lastActivityTime(), spMinInfo, tier);

        sink.add(currentSample, interpolatedVCU, usageMetadata, backfillTimestamp);
    }

    @Nullable
    static SampledVCUMetricsProvider.SPMinInfo tryExtractSPMinInfo(MetricValue sample) {
        var spMinProvisioned = getSpMinProvisionedMemory(sample);
        if (spMinProvisioned != null) {
            return new SampledVCUMetricsProvider.SPMinInfo(spMinProvisioned, getSpMin(sample), getSpMinStorageRamRatio(sample));
        }
        return null;
    }

    @Nullable
    static SampledVCUMetricsProvider.SPMinInfo tryInterpolateSPMinInfo(MetricValue currentSample, MetricValue previousSample) {
        var currentSpMinProvisioned = getSpMinProvisionedMemory(currentSample);
        var previousSpMinProvisioned = getSpMinProvisionedMemory(previousSample);
        if (previousSpMinProvisioned != null && currentSpMinProvisioned != null) {
            // If sp_min_provisioned_memory is present sp_min and sp_min_storage_ram_ratio are also present
            // Return the values associated with the smallest sp_min_provisioned_memory
            return previousSpMinProvisioned < currentSpMinProvisioned
                ? new SampledVCUMetricsProvider.SPMinInfo(
                    previousSpMinProvisioned,
                    getSpMin(previousSample),
                    getSpMinStorageRamRatio(previousSample)
                )
                : new SampledVCUMetricsProvider.SPMinInfo(
                    currentSpMinProvisioned,
                    getSpMin(currentSample),
                    getSpMinStorageRamRatio(currentSample)
                );
        }
        return null;
    }

    Activity.ActiveInfo inferActivity(
        Activity activity,
        @Nullable Instant previousLastActivity,
        Instant backfillTimestamp,
        Duration coolDown,
        String tier,
        BackfillType backfillType
    ) {
        assert previousLastActivity == null || backfillTimestamp.isBefore(previousLastActivity) == false;

        // First we check if backfillTimestamp is covered by current activity
        var currentActiveInfo = activity.wasActive(backfillTimestamp, coolDown);
        if (currentActiveInfo.isPresent()) {
            return currentActiveInfo.get();
        } else {
            // backfillTimestamp was not covered by current activity, there are a few reasons why this could have happened

            // 1) activity is empty because there have been no actions, possibly due to a rolling restart
            if (activity.isEmpty()) {
                logger.debug("Reporting inactivity due to empty data for backfill time: {}", backfillTimestamp);
                defaultActivityReturnedCounter.incrementBy(1L, makeAttributes(tier, backfillType, DefaultReason.EMPTY_ACTIVITY));
                return Activity.DEFAULT_NOT_ACTIVE;
            }

            // 2) previous sample does not contain last activity, possibly due to the metering persistent task moving nodes
            if (previousLastActivity == null) {
                logger.debug("Reporting inactivity because previousLastActivity is missing, backfill time: {}", backfillTimestamp);
                defaultActivityReturnedCounter.incrementBy(1L, makeAttributes(tier, backfillType, DefaultReason.MISSING_PREVIOUS));
                return Activity.DEFAULT_NOT_ACTIVE;
            }

            // 3) backfillTimestamp is before activity.firstActivityPreviousPeriod but after previousLastActivity + coolDown
            if (backfillTimestamp.isAfter(previousLastActivity.plus(coolDown))) {
                logger.debug("Reporting inactivity because tracked activity does not cover backfill time: {}", backfillTimestamp);
                defaultActivityReturnedCounter.incrementBy(1L, makeAttributes(tier, backfillType, DefaultReason.NOT_ENOUGH_PERIODS));
                return Activity.DEFAULT_NOT_ACTIVE;
            }

            // 4) backfillTimestamp is before previousLastActivity + coolDown (and as always, after previousLastActivity)
            return new Activity.ActiveInfo(true, previousLastActivity);
        }
    }

    private static Map<String, Object> makeAttributes(String tier, BackfillType backfillType, DefaultReason reason) {
        return Map.of("tier", tier, "backfill-type", backfillType.name(), "reason", reason.name());
    }

    private static Instant getPreviousLastActivityTime(MetricValue metricValue) {
        String lastActivityTime = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_LATEST_ACTIVITY_TIME);
        return lastActivityTime == null ? null : Instant.parse(lastActivityTime);
    }

    private static Long getSpMinProvisionedMemory(MetricValue metricValue) {
        String spMinProvisionedMem = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY);
        return spMinProvisionedMem == null ? null : Long.parseLong(spMinProvisionedMem);
    }

    private static long getSpMin(MetricValue metricValue) {
        String spMin = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN);
        assert spMin != null : "sp_min should be present";
        return Long.parseLong(spMin);
    }

    private static double getSpMinStorageRamRatio(MetricValue metricValue) {
        String ratio = metricValue.usageMetadata().get(SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO);
        assert ratio != null : "sp_min_storage_ram_ratio should be present";
        return Double.parseDouble(ratio);
    }
}
