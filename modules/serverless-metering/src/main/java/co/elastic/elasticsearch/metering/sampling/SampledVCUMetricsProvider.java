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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.os.OsProbe;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
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

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final Function<SampledClusterMetrics, SPMinInfo> spMinProvisionedMemoryProvider;
    private final Duration activityCoolDownPeriod;

    SampledVCUMetricsProvider(
        SampledClusterMetricsService sampledClusterMetricsService,
        Duration activityCoolDownPeriod,
        Function<SampledClusterMetrics, SPMinInfo> spMinProvisionedMemoryProvider
    ) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.activityCoolDownPeriod = activityCoolDownPeriod;
        this.spMinProvisionedMemoryProvider = spMinProvisionedMemoryProvider;
    }

    @Override
    public Optional<MetricValues> getMetrics() {
        return sampledClusterMetricsService.withSamplesIfReady((SampledClusterMetrics sample) -> {
            boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
            List<MetricValue> metrics = List.of(
                buildMetricValue(
                    spMinProvisionedMemoryProvider.apply(sample),
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
                    activityCoolDownPeriod
                )
            );
        });
    }

    private static MetricValue buildMetricValue(
        SPMinInfo spMinInfo,
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

    public static Map<String, String> buildUsageMetadata(boolean isActive, Instant lastActivityTime, SPMinInfo spMinInfo, String tier) {
        Map<String, String> usageMetadata = new HashMap<>();
        usageMetadata.put(USAGE_METADATA_APPLICATION_TIER, tier);
        usageMetadata.put(USAGE_METADATA_ACTIVE, Boolean.toString(isActive));
        if (lastActivityTime.equals(Instant.EPOCH) == false) {
            usageMetadata.put(USAGE_METADATA_LATEST_ACTIVITY_TIME, lastActivityTime.toString());
        }
        if (spMinInfo != null) {
            usageMetadata.put(USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY, Long.toString(spMinInfo.provisionedMemory));
            usageMetadata.put(USAGE_METADATA_SP_MIN, Long.toString(spMinInfo.spMin));
        }
        return usageMetadata;
    }

    record SPMinInfo(long provisionedMemory, long spMin) {};

    static class SPMinProvisionedMemoryProvider implements Function<SampledClusterMetrics, SPMinInfo> {
        private final long provisionedStorage;
        private final long provisionedRAM;
        private volatile long searchPowerMin;

        SPMinProvisionedMemoryProvider(ClusterService clusterService, long provisionedStorage, long provisionedRAM) {
            assert provisionedStorage > 0;
            assert provisionedRAM > 0;
            this.provisionedStorage = provisionedStorage;
            this.provisionedRAM = provisionedRAM;
            clusterService.getClusterSettings().initializeAndWatch(SEARCH_POWER_MIN_SETTING, v -> this.searchPowerMin = v);
        }

        public static Function<SampledClusterMetrics, SPMinInfo> build(ClusterService clusterService, NodeEnvironment nodeEnvironment) {
            return build(
                clusterService,
                () -> new FsService(clusterService.getSettings(), nodeEnvironment).stats().getTotal().getTotal().getBytes(),
                () -> OsProbe.getInstance().getTotalPhysicalMemorySize()
            );
        }

        static Function<SampledClusterMetrics, SPMinInfo> build(
            ClusterService clusterService,
            Supplier<Long> storageSupplier,
            Supplier<Long> ramSupplier
        ) {
            boolean isSearchNode = DiscoveryNode.hasRole(clusterService.getSettings(), DiscoveryNodeRole.SEARCH_ROLE);
            if (isSearchNode == false) {
                return errorProvider(
                    "sp_min_provisioned_memory can only be computed on a search node. The metering persistent task is only run "
                        + "on search nodes, so this should not occur."
                );
            }

            long provisionedStorage = storageSupplier.get();
            if (provisionedStorage <= 0) {
                return errorProvider("provisionedStorage must be greater than zero, but values is: " + provisionedStorage);
            }

            long provisionedRAM = ramSupplier.get();
            if (provisionedRAM <= 0) {
                return errorProvider("provisionedRAM must be greater than zero, but values is: " + provisionedRAM);
            }

            return new SPMinProvisionedMemoryProvider(clusterService, provisionedStorage, provisionedRAM);
        }

        private static Function<SampledClusterMetrics, SPMinInfo> errorProvider(String message) {
            return current -> {
                logger.error(message);
                return null;
            };
        }

        @Override
        public SPMinInfo apply(SampledClusterMetrics currentInfo) {
            long spMin = searchPowerMin;
            long boostedDataSetSize = 0;
            long totalDataSetSize = 0;
            for (var sample : currentInfo.shardSamples().values()) {
                var shardInfo = sample.shardInfo();
                boostedDataSetSize += shardInfo.interactiveSizeInBytes();
                totalDataSetSize += shardInfo.totalSizeInBytes();
            }

            double storageRamRatio = provisionedStorage / (double) provisionedRAM;
            double basePower = 0.05 * spMin / 100.0;
            double boostPower = spMin / 100.0 - basePower;
            double cacheSize = boostedDataSetSize * boostPower + totalDataSetSize * basePower;
            long provisionedMemory = (long) (cacheSize / storageRamRatio);
            return new SPMinInfo(provisionedMemory, spMin);
        }
    }
}
