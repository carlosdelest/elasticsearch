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
import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.os.OsProbe;

import java.util.Map;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;

class SPMinProvisionedMemoryCalculator {
    private static final Logger logger = LogManager.getLogger(SPMinProvisionedMemoryCalculator.class);

    record SPMinInfo(long provisionedMemory, long spMin, double storageRamRatio) {
        void appendToUsageMetadata(Map<String, String> usageMetadata) {
            usageMetadata.put(UsageMetadata.SP_MIN_PROVISIONED_MEMORY, Long.toString(provisionedMemory));
            usageMetadata.put(UsageMetadata.SP_MIN, Long.toString(spMin));
            usageMetadata.put(UsageMetadata.SP_MIN_STORAGE_RAM_RATIO, Strings.format1Decimals(storageRamRatio, ""));
        }

        static SPMinInfo minOf(MetricValue sampleA, MetricValue sampleB) {
            var memoryA = provisionedMemory(sampleA);
            var memoryB = provisionedMemory(sampleB);
            // Return the values associated with the smallest sp_min_provisioned_memory
            return memoryA <= memoryB ? create(memoryA, sampleA) : create(memoryB, sampleB);
        }

        private static SPMinInfo create(long provisionedMemory, MetricValue sample) {
            if (provisionedMemory == Long.MAX_VALUE) {
                return null; // fallback if sp_min_provisioned_memory is not present
            }
            String spMin = sample.usageMetadata().get(UsageMetadata.SP_MIN);
            String ratio = sample.usageMetadata().get(UsageMetadata.SP_MIN_STORAGE_RAM_RATIO);
            assert spMin != null : "sp_min should be present";
            assert ratio != null : "sp_min_storage_ram_ratio should be present";
            return new SPMinInfo(provisionedMemory, Long.parseLong(spMin), Double.parseDouble(ratio));
        }

        private static long provisionedMemory(MetricValue sample) {
            String memory = sample.usageMetadata().get(UsageMetadata.SP_MIN_PROVISIONED_MEMORY);
            return memory == null ? Long.MAX_VALUE : Long.parseLong(memory);
        }
    }

    static SPMinProvisionedMemoryCalculator build(
        ClusterService clusterService,
        SystemIndices systemIndices,
        NodeEnvironment nodeEnvironment
    ) {
        return build(
            clusterService,
            systemIndices,
            new FsService(clusterService.getSettings(), nodeEnvironment).stats().getTotal().getTotal().getBytes(),
            OsProbe.getInstance().getTotalPhysicalMemorySize()
        );
    }

    static SPMinProvisionedMemoryCalculator build(
        ClusterService clusterService,
        SystemIndices systemIndices,
        long provisionedStorage,
        long provisionedRAM
    ) {
        if (provisionedStorage <= 0) {
            throw new IllegalStateException("provisionedStorage must be greater than zero, but values is: " + provisionedStorage);
        }
        if (provisionedRAM <= 0) {
            throw new IllegalStateException("provisionedRAM must be greater than zero, but values is: " + provisionedRAM);
        }
        return new SPMinProvisionedMemoryCalculator(clusterService, systemIndices, provisionedStorage, provisionedRAM);
    }

    private final SystemIndices systemIndices;
    private final long provisionedStorage;
    private final long provisionedRAM;
    private volatile long searchPowerMin;

    private SPMinProvisionedMemoryCalculator(
        ClusterService clusterService,
        SystemIndices systemIndices,
        long provisionedStorage,
        long provisionedRAM
    ) {
        assert provisionedStorage > 0;
        assert provisionedRAM > 0;
        this.systemIndices = systemIndices;
        this.provisionedStorage = provisionedStorage;
        this.provisionedRAM = provisionedRAM;
        clusterService.getClusterSettings().initializeAndWatch(SEARCH_POWER_MIN_SETTING, v -> this.searchPowerMin = v);
    }

    public SPMinInfo calculate(long boostedDataSetSize, long totalDataSetSize) {
        long spMin = searchPowerMin;
        double storageRamRatio = provisionedStorage / (double) provisionedRAM;
        double basePower = 0.05 * spMin / 100.0;
        double boostPower = spMin / 100.0 - basePower;
        double cacheSize = boostedDataSetSize * boostPower + totalDataSetSize * basePower;
        long provisionedMemory = (long) (cacheSize / storageRamRatio);
        return new SPMinInfo(provisionedMemory, spMin, storageRamRatio);
    }

    public SPMinInfo calculate(SampledClusterMetricsService.SampledClusterMetrics currentInfo) {
        long boostedDataSetSize = 0;
        long totalDataSetSize = 0;
        for (var entry : currentInfo.shardSamples().entrySet()) {
            if (systemIndices.isSystemIndex(entry.getKey().indexName())) {
                continue; // temporarily skip system indices until VCU for inactivity is reported by index
            }
            var shardInfo = entry.getValue().shardInfo();
            boostedDataSetSize += shardInfo.interactiveSizeInBytes();
            totalDataSetSize += shardInfo.totalSizeInBytes();
        }
        SPMinInfo spMinInfo = calculate(boostedDataSetSize, totalDataSetSize);

        if (spMinInfo.provisionedMemory > currentInfo.searchTierMetrics().memorySize()) {
            logger.warn(
                "spMinProvisionedMemory [{}] for inactivity billing exceeded actual provisioned search tier memory [{}] "
                    + "[spMin: {}, storage: {}, memory: {}, interactiveData: {}, totalData: {}]",
                ByteSizeValue.ofBytes(spMinInfo.provisionedMemory),
                ByteSizeValue.ofBytes(currentInfo.searchTierMetrics().memorySize()),
                spMinInfo.spMin,
                ByteSizeValue.ofBytes(provisionedStorage),
                ByteSizeValue.ofBytes(provisionedRAM),
                ByteSizeValue.ofBytes(boostedDataSetSize),
                ByteSizeValue.ofBytes(totalDataSetSize)
            );
        } else if (logger.isTraceEnabled()) {
            logger.trace(
                "spMinProvisionedMemory: {} [spMin: {}, storage: {}, memory: {}, interactiveData: {}, totalData: {}]",
                ByteSizeValue.ofBytes(spMinInfo.provisionedMemory),
                spMinInfo.spMin,
                ByteSizeValue.ofBytes(provisionedStorage),
                ByteSizeValue.ofBytes(provisionedRAM),
                ByteSizeValue.ofBytes(boostedDataSetSize),
                ByteSizeValue.ofBytes(totalDataSetSize)
            );
        }
        return spMinInfo;
    }
}
