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

import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

class SampledVCUMetricsProvider implements SampledMetricsProvider {
    static final String VCU_METRIC_TYPE = "es_vcu";
    static final String VCU_METRIC_ID_PREFIX = "vcu";
    static final String METADATA_PARTIAL_KEY = "partial";
    static final String USAGE_METADATA_APPLICATION_TIER = "application_tier";
    static final String USAGE_METADATA_ACTIVE = "active";
    static final String USAGE_METADATA_LATEST_ACTIVITY_TIME = "latest_activity_timestamp";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final Duration activityCoolDownPeriod;

    SampledVCUMetricsProvider(SampledClusterMetricsService sampledClusterMetricsService, Duration activityCoolDownPeriod) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
    }

    @Override
    public Optional<MetricValues> getMetrics() {
        return sampledClusterMetricsService.withSamplesIfReady((SampledClusterMetricsService.SampledClusterMetrics sample) -> {
            boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
            List<MetricValue> metrics = List.of(
                buildMetricValue(sample.searchTierMetrics(), "search", activityCoolDownPeriod, partial),
                buildMetricValue(sample.indexTierMetrics(), "index", activityCoolDownPeriod, partial)
            );
            return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
        });
    }

    private static MetricValue buildMetricValue(
        SampledClusterMetricsService.SampledTierMetrics tierMetrics,
        String tier,
        Duration coolDown,
        boolean partial
    ) {
        boolean isActive = tierMetrics.activity().isActive(Instant.now(), coolDown);
        Instant lastActivityTime = tierMetrics.activity().lastActivityRecentPeriod();

        Map<String, String> usageMetadata = new HashMap<>();
        usageMetadata.put(USAGE_METADATA_APPLICATION_TIER, tier);
        usageMetadata.put(USAGE_METADATA_ACTIVE, Boolean.toString(isActive));
        if (lastActivityTime.equals(Instant.EPOCH) == false) {
            usageMetadata.put(USAGE_METADATA_LATEST_ACTIVITY_TIME, lastActivityTime.toString());
        }

        return new MetricValue(
            format("%s:%s", VCU_METRIC_ID_PREFIX, tier),
            VCU_METRIC_TYPE,
            partial ? Map.of(METADATA_PARTIAL_KEY, Boolean.TRUE.toString()) : Map.of(),
            usageMetadata,
            tierMetrics.memorySize(),
            null
        );
    }
}
