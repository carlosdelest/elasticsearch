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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.os.OsProbe;

import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;

interface SPMinProvisionedMemoryCalculator {
    Logger logger = LogManager.getLogger(SPMinProvisionedMemoryCalculator.class);

    record SPMinInfo(long provisionedMemory, long spMin, double storageRamRatio) {}

    SPMinInfo calculate(long boostedDataSetSize, long totalDataSetSize);

    SPMinInfo calculate(SampledClusterMetricsService.SampledClusterMetrics currentInfo);

    static SPMinProvisionedMemoryCalculator build(
        ClusterService clusterService,
        SystemIndices systemIndices,
        NodeEnvironment nodeEnvironment
    ) {
        return build(
            clusterService,
            systemIndices,
            () -> new FsService(clusterService.getSettings(), nodeEnvironment).stats().getTotal().getTotal().getBytes(),
            () -> OsProbe.getInstance().getTotalPhysicalMemorySize()
        );
    }

    static SPMinProvisionedMemoryCalculator build(
        ClusterService clusterService,
        SystemIndices systemIndices,
        Supplier<Long> storageSupplier,
        Supplier<Long> ramSupplier
    ) {
        boolean isSearchNode = DiscoveryNode.hasRole(clusterService.getSettings(), DiscoveryNodeRole.SEARCH_ROLE);
        if (isSearchNode == false) {
            return loggingNoopCalculator(
                "sp_min_provisioned_memory can only be computed on a search node. The metering persistent task is only run "
                    + "on search nodes, so this should not occur."
            );
        }

        long provisionedStorage = storageSupplier.get();
        if (provisionedStorage <= 0) {
            return loggingNoopCalculator("provisionedStorage must be greater than zero, but values is: " + provisionedStorage);
        }

        long provisionedRAM = ramSupplier.get();
        if (provisionedRAM <= 0) {
            return loggingNoopCalculator("provisionedRAM must be greater than zero, but values is: " + provisionedRAM);
        }

        return new SPMinProvisionedMemoryCalculatorImpl(clusterService, systemIndices, provisionedStorage, provisionedRAM);
    }

    private static SPMinProvisionedMemoryCalculator loggingNoopCalculator(String message) {
        return new SPMinProvisionedMemoryCalculator() {
            @Override
            public SPMinInfo calculate(long boostedDataSetSize, long totalDataSetSize) {
                logger.error(message);
                return null;
            }

            @Override
            public SPMinInfo calculate(SampledClusterMetricsService.SampledClusterMetrics currentInfo) {
                logger.error(message);
                return null;
            }
        };
    }

    class SPMinProvisionedMemoryCalculatorImpl implements SPMinProvisionedMemoryCalculator {

        private final SystemIndices systemIndices;
        private final long provisionedStorage;
        private final long provisionedRAM;
        private volatile long searchPowerMin;

        SPMinProvisionedMemoryCalculatorImpl(
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

        @Override
        public SPMinInfo calculate(long boostedDataSetSize, long totalDataSetSize) {
            long spMin = searchPowerMin;
            double storageRamRatio = provisionedStorage / (double) provisionedRAM;
            double basePower = 0.05 * spMin / 100.0;
            double boostPower = spMin / 100.0 - basePower;
            double cacheSize = boostedDataSetSize * boostPower + totalDataSetSize * basePower;
            long provisionedMemory = (long) (cacheSize / storageRamRatio);
            return new SPMinInfo(provisionedMemory, spMin, storageRamRatio);
        }

        @Override
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

}
