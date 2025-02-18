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

import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

public class SampledVCUMetricsProvider implements SampledMetricsProvider {

    private static final Logger logger = LogManager.getLogger(SampledVCUMetricsProvider.class);
    static final String VCU_METRIC_TYPE = "es_vcu";
    static final String VCU_METRIC_ID_PREFIX = "vcu";
    static final String METADATA_PARTIAL_KEY = "partial";
    public static final String USAGE_METADATA_APPLICATION_TIER = "application_tier";
    public static final String USAGE_METADATA_ACTIVE = "active";
    public static final String USAGE_METADATA_LATEST_ACTIVITY_TIME = "latest_activity_timestamp";
    public static final String USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY = "sp_min_provisioned_memory";
    public static final String USAGE_METADATA_SP_MIN = "sp_min";
    public static final String USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO = "sp_min_storage_ram_ratio";
    public static final String METERING_REPORTING_BACKFILL_ACTIVITY_UNKNOWN = "es.metering.reporting.backfill.activity.unknown.total";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final SPMinProvisionedMemoryCalculator spMinProvisionedMemoryCalculator;
    private final Duration activityCoolDownPeriod;
    private final LongCounter defaultActivityReturnedCounter;

    SampledVCUMetricsProvider(
        SampledClusterMetricsService sampledClusterMetricsService,
        Duration activityCoolDownPeriod,
        SPMinProvisionedMemoryCalculator spMinProvisionedMemoryCalculator,
        MeterRegistry meterRegistry
    ) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
        this.spMinProvisionedMemoryCalculator = spMinProvisionedMemoryCalculator;
        this.defaultActivityReturnedCounter = meterRegistry.registerLongCounter(
            METERING_REPORTING_BACKFILL_ACTIVITY_UNKNOWN,
            "The number of activity backfill attempts where activity data was not present so default value was returned.",
            "unit"
        );
    }

    @Override
    public Optional<MetricValues> getMetrics() {
        return sampledClusterMetricsService.withSamplesIfReady((SampledClusterMetrics sample) -> {
            boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
            List<MetricValue> metrics = List.of(
                buildMetricValue(
                    spMinProvisionedMemoryCalculator.calculate(sample),
                    sample.searchTierMetrics(),
                    "search",
                    activityCoolDownPeriod,
                    partial
                ),
                buildMetricValue(null, sample.indexTierMetrics(), "index", activityCoolDownPeriod, partial)
            );
            return SampledMetricsProvider.metricValues(
                metrics,
                new VCUSampledMetricsBackfillStrategy(
                    sample.searchTierMetrics().activity(),
                    sample.indexTierMetrics().activity(),
                    activityCoolDownPeriod,
                    defaultActivityReturnedCounter
                )
            );
        }, status -> logger.warn("Samples not ready metrics collection [sampling node: {}]", status));
    }

    private static MetricValue buildMetricValue(
        SPMinProvisionedMemoryCalculator.SPMinInfo spMinInfo,
        SampledClusterMetricsService.SampledTierMetrics tierMetrics,
        String tier,
        Duration coolDown,
        boolean partial
    ) {
        boolean isActive = tierMetrics.activity().isActive(Instant.now(), coolDown);
        Instant lastActivityTime = tierMetrics.activity().lastActivityRecentPeriod();
        var usageMetadata = buildUsageMetadata(isActive, lastActivityTime, spMinInfo, tier);
        return new MetricValue(
            format("%s:%s", VCU_METRIC_ID_PREFIX, tier),
            VCU_METRIC_TYPE,
            partial ? Map.of(METADATA_PARTIAL_KEY, Boolean.TRUE.toString()) : Map.of(),
            usageMetadata,
            tierMetrics.memorySize(),
            null
        );
    }

    public static Map<String, String> buildUsageMetadata(
        boolean isActive,
        Instant lastActivityTime,
        SPMinProvisionedMemoryCalculator.SPMinInfo spMinInfo,
        String tier
    ) {
        Map<String, String> usageMetadata = new HashMap<>();
        usageMetadata.put(USAGE_METADATA_APPLICATION_TIER, tier);
        usageMetadata.put(USAGE_METADATA_ACTIVE, Boolean.toString(isActive));
        if (lastActivityTime.equals(Instant.EPOCH) == false) {
            usageMetadata.put(USAGE_METADATA_LATEST_ACTIVITY_TIME, lastActivityTime.toString());
        }
        if (spMinInfo != null) {
            usageMetadata.put(USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY, Long.toString(spMinInfo.provisionedMemory()));
            usageMetadata.put(USAGE_METADATA_SP_MIN, Long.toString(spMinInfo.spMin()));
            usageMetadata.put(USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO, Strings.format1Decimals(spMinInfo.storageRamRatio(), ""));
        }
        return usageMetadata;
    }
}
