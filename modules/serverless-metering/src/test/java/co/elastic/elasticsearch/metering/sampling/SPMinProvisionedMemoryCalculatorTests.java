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

import co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SPMinProvisionedMemoryCalculatorTests extends ESTestCase {

    private static final long DEFAULT_SP_MIN = 100;
    private ClusterService clusterService;
    private SystemIndices systemIndices;

    @Before
    public void initMocks() {
        clusterService = mockClusterService(true);
        systemIndices = mock();
    }

    public void testSpMinProvisionedMemoryBadValue() {
        assertThrows(
            IllegalStateException.class,
            () -> SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, randomNonNegativeLong(), randomLongBetween(-5, 0))
        );
    }

    public void testSpMinProvisionedMemoryStorageBadValue() {
        assertThrows(
            IllegalStateException.class,
            () -> SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, randomLongBetween(-5, 0), randomNonNegativeLong())
        );
    }

    public void testSpMinProvisionedMemoryNoShardsReturn0() {
        var clusterService = mockClusterService(true);
        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, Map.of(), Set.of());
        var calculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, 10, 10);
        assertThat(calculator.calculate(current).provisionedMemory(), equalTo(0L));
    }

    // TODO Should we remove this behavior and this test?
    public void testSpMinProvisionedMemoryNegativeBoost() {
        // spMin = 1, interactiveSize = 100, totalSize = 100, provisionedStorage = 1000, provisionedRam = 1000
        // storageRamRatio = 1, since provisionedStorage and provisionedRam are equal
        // basePower = 5, since spMin=1 falls back to MINIMUM_SEARCH_POWER
        // boostPower = -4, spMin - basePower
        // cacheSize = 100, boostSize * boostPower + totalSize * basePower, 100 * -4 + 100 * 5
        assertThat(getSPMinProvisionedMemory(1, 100, 100, 1000, 1000), equalTo(1L));
    }

    public void testSpMinProvisionedMemorySearchPower0() {
        // spMin = 0, interactiveSize = 100, totalSize = 100, provisionedStorage = 1000, provisionedRam = 1000
        // storageRamRatio = 1, since provisionedStorage and provisionedRam are equal
        // basePower = 5, since spMin=0 falls back to MINIMUM_SEARCH_POWER
        // boostPower = -5, spMin - basePower
        // cacheSize = 0, boostSize * boostPower + totalSize * basePower, 100 * -5 + 100 * 5
        assertThat(getSPMinProvisionedMemory(0, 100, 100, 1000, 1000), equalTo(0L));
    }

    public void testSpMinProvisionedMemoryCostEfficientProject() {
        // 0 SP min for cost-efficient projects to not charge for inactivity
        // spMin = 0, interactiveSize = 100, totalSize = 200, provisionedStorage = 1000, provisionedRam = 1000
        // storageRamRatio = 1, since provisionedStorage and provisionedRam are equal
        // basePower = 5, since default SPMin is small, and min base power is 5
        // boostPower = 0, SPMin - basePower
        // cacheSize = 1000, totalSize * basePower, 200 * 5
        assertThat(getSPMinProvisionedMemory(0, 100, 200, 1000, 1000), equalTo(0L));
    }

    public void testSpMinProvisionedMemoryStorageRatioDefaultSPMin() {
        // spMin = 100, interactiveSize = 100, totalSize = 100, provisionedStorage = 1000, provisionedRam = 1000
        // storageRamRatio = 1, 1000/1000
        // basePower = 5, since default SPMin is 100
        // boostPower = 95, SPMin - basePower
        // cacheSize = 10500, 100 * 95 + 200 * 5
        assertThat(getSPMinProvisionedMemory(null, 100, 200, 1000, 1000), equalTo(105L));
    }

    public void testSpMinProvisionedMemoryStorageRatioD() {
        // spMin = 100, interactiveSize = 100, totalSize = 200, provisionedStorage = 2000, provisionedRam = 1000
        // storageRamRatio = 2, 2000/1000
        // basePower = 5, since default SPMin is 100
        // boostPower = 95, SPMin - basePower
        // cacheSize = 10500, 100 * 95 + 200 * 5
        // result = 10500 / 2 / 100
        assertThat(getSPMinProvisionedMemory(null, 100, 200, 2000, 1000), equalTo(52L));
    }

    public void testSpMinProvisionedMemorySkipSystemIndices() {
        var clusterService = mockClusterService(true);
        when(systemIndices.isSystemIndex("my_system_index")).thenReturn(true);

        var shardSamples = Map.of(
            new SampledClusterMetricsService.ShardKey("regular_index", 0),
            randomShardSample(500, 1000),
            new SampledClusterMetricsService.ShardKey("my_system_index", 0),
            randomShardSample(200, 200)

        );
        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardSamples, Set.of());

        long provisionedRam = 1000;
        long storageRamRatio = 10;
        long provisionedStorage = storageRamRatio * provisionedRam;
        var calculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, provisionedStorage, provisionedRam);

        // basePower = 0.05, since default SPMin is 100
        // boostPower = 0.95, SPMin/100 - basePower
        // cacheSize = 525, boostSize * boostPower + totalSize * basePower
        double cacheSize = 500 * 0.95 + 1000 * 0.05;
        double expectSPMinRam = cacheSize / storageRamRatio;
        assertThat((double) calculator.calculate(current).provisionedMemory(), closeTo(expectSPMinRam, 1.0));
    }

    public void testSpMinProvisionedMemoryLotsOfShards() {
        var clusterService = mockClusterService(true);

        long totalInteractive = 0;
        long totalSize = 0;
        int numShards = between(1, 100);
        Map<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> shardSamples = new HashMap<>();
        for (int i = 0; i < numShards; ++i) {
            long interactiveSize = between(1, 10_000);
            long nonInteractiveSize = between(1, 10_000);
            totalInteractive += interactiveSize;
            totalSize += interactiveSize + nonInteractiveSize;
            shardSamples.put(randomShardKey(), randomShardSample(interactiveSize, interactiveSize + nonInteractiveSize));
        }

        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardSamples, Set.of());

        long provisionedRam = between(1, 1000);
        long storageRamRatio = between(1, 10);
        long provisionedStorage = storageRamRatio * provisionedRam;
        var calculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, provisionedStorage, provisionedRam);

        // basePower = 0.05, since default SPMin is 100
        // boostPower = 0.95, SPMin/100 - basePower
        // cacheSize = 10500, boostSize * boostPower + totalSize * basePower, 100 * 95 + 200 * 5
        double cacheSize = totalInteractive * 0.95 + totalSize * 0.05;
        double expectSPMinRam = cacheSize / storageRamRatio;

        assertThat((double) calculator.calculate(current).provisionedMemory(), closeTo(expectSPMinRam, 1.0));
        assertThat((double) calculator.calculate(totalInteractive, totalSize).provisionedMemory(), closeTo(expectSPMinRam, 1.0));
    }

    private Long getSPMinProvisionedMemory(
        Integer spMin,
        long interactiveSize,
        long totalSize,
        long provisionedStorage,
        long provisionedRam
    ) {
        var clusterService = mockClusterService(true, spMin);
        var shardSamples = Map.of(randomShardKey(), randomShardSample(interactiveSize, totalSize));
        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardSamples, Set.of());
        var calculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, provisionedStorage, provisionedRam);
        long provisionedMemory = calculator.calculate(interactiveSize, totalSize).provisionedMemory();
        assertThat(provisionedMemory, is(calculator.calculate(current).provisionedMemory()));
        return provisionedMemory;
    }

    private static Function<SampledClusterMetrics, SPMinProvisionedMemoryCalculator.SPMinInfo> buildSpMinTestcalculator(
        Long... provisionedMem
    ) {
        var spMinInfos = Arrays.stream(provisionedMem)
            .map(v -> new SPMinProvisionedMemoryCalculator.SPMinInfo(v, DEFAULT_SP_MIN, 1.0))
            .toList();
        return buildSpMinTestcalculator(spMinInfos);
    }

    private static Function<SampledClusterMetrics, SPMinProvisionedMemoryCalculator.SPMinInfo> buildSpMinTestcalculator(
        List<SPMinProvisionedMemoryCalculator.SPMinInfo> spMinInfos
    ) {
        var iter = spMinInfos.iterator();
        return current -> {
            assert iter.hasNext() : "Cannot call calculator with more than number of provided values";
            return iter.next();
        };
    }

    private static ClusterService mockClusterService(boolean isSearchNode) {
        return mockClusterService(isSearchNode, null);
    }

    private static ClusterService mockClusterService(boolean isSearchNode, Integer spMin) {
        var clusterService = mock(ClusterService.class);

        var builder = Settings.builder();
        if (spMin != null) {
            builder.put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin);
        }
        builder.put("node.roles", isSearchNode ? DiscoveryNodeRole.SEARCH_ROLE.roleName() : DiscoveryNodeRole.INDEX_ROLE.roleName());

        var settings = builder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);

        return clusterService;
    }

    private SampledClusterMetricsService.ShardKey randomShardKey() {
        return new SampledClusterMetricsService.ShardKey(randomUUID(), randomInt());
    }

    private static SampledClusterMetricsService.ShardSample randomShardSample(long interactiveSizeInBytes, long totalSizeInBytes) {
        var nonInteractiveSizeInBytes = totalSizeInBytes - interactiveSizeInBytes;
        return new SampledClusterMetricsService.ShardSample(
            randomUUID(),
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(0, interactiveSizeInBytes, nonInteractiveSizeInBytes, 0).build()
        );
    }
}
