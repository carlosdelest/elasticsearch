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

import co.elastic.elasticsearch.metering.SourceMetadata;
import co.elastic.elasticsearch.metering.UsageMetadata;
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

public class SampledVCUMetricsProvider implements SampledMetricsProvider {
    private static final Logger logger = LogManager.getLogger(SampledVCUMetricsProvider.class);

    static final String VCU_METRIC_TYPE = "es_vcu";
    static final String VCU_METRIC_ID_PREFIX = "vcu";

    public static final String METERING_REPORTING_BACKFILL_ACTIVITY_UNKNOWN = "es.metering.reporting.backfill.activity.unknown.total";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final SPMinProvisionedMemoryCalculator spMinProvisionedMemoryCalculator;
    private final LongCounter defaultActivityReturnedCounter;

    SampledVCUMetricsProvider(
        SampledClusterMetricsService sampledClusterMetricsService,
        SPMinProvisionedMemoryCalculator spMinProvisionedMemoryCalculator,
        MeterRegistry meterRegistry
    ) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
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
                buildMetricValue(spMinProvisionedMemoryCalculator.calculate(sample), sample.searchTierMetrics(), "search", partial),
                buildMetricValue(null, sample.indexTierMetrics(), "index", partial)
            );
            return SampledMetricsProvider.metricValues(
                metrics,
                new VCUSampledMetricsBackfillStrategy(
                    sample.searchTierMetrics().activity(),
                    sample.indexTierMetrics().activity(),
                    sampledClusterMetricsService.activityCoolDownPeriod(),
                    defaultActivityReturnedCounter
                )
            );
        }, status -> logger.warn("Samples not ready metrics collection [sampling node: {}]", status));
    }

    private MetricValue buildMetricValue(
        SPMinProvisionedMemoryCalculator.SPMinInfo spMinInfo,
        SampledClusterMetricsService.SampledTierMetrics tierMetrics,
        String tier,
        boolean partial
    ) {
        var activitySnapshot = sampledClusterMetricsService.activitySnapshot(tierMetrics);
        var usageMetadata = new HashMap<String, String>();
        updateUsageMetadata(usageMetadata, activitySnapshot, spMinInfo, tier);
        return new MetricValue(
            format("%s:%s", VCU_METRIC_ID_PREFIX, tier),
            VCU_METRIC_TYPE,
            partial ? Map.of(SourceMetadata.PARTIAL, Boolean.TRUE.toString()) : Map.of(),
            usageMetadata,
            tierMetrics.memorySize(),
            null
        );
    }

    static void updateUsageMetadata(
        Map<String, String> usageMetadata,
        Activity.Snapshot activitySnapshot,
        SPMinProvisionedMemoryCalculator.SPMinInfo spMinInfo,
        String tier
    ) {
        usageMetadata.put(UsageMetadata.APPLICATION_TIER, tier);
        activitySnapshot.appendToUsageMetadata(usageMetadata, UsageMetadata.ACTIVE, UsageMetadata.LATEST_ACTIVITY_TIMESTAMP);
        if (spMinInfo != null) {
            spMinInfo.appendToUsageMetadata(usageMetadata);
        }
    }
}
