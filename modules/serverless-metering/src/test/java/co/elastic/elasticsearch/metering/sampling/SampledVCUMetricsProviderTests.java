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
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static co.elastic.elasticsearch.metering.TestUtils.hasBackfillStrategy;
import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.SPMinInfo;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.SPMinProvisionedMemoryProvider;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_ACTIVE;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_LATEST_ACTIVITY_TIME;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledVCUMetricsProviderTests extends ESTestCase {

    private static final long DEFAULT_SP_MIN = 100;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mockClusterService(true);
    }

    private void setMetricsServiceData(
        SampledClusterMetricsService metricsService,
        SampledTierMetrics searchTierMetrics,
        SampledTierMetrics indexTierMetrics
    ) {
        metricsService.collectedMetrics.set(new SampledClusterMetrics(searchTierMetrics, indexTierMetrics, Map.of(), Set.of()));
        metricsService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
    }

    private static AssertionError elementMustBePresent() {
        return new AssertionError("Element must be present");
    }

    public void testGetMetricsEmptyActivity() {

        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var spMinProvisionedMemory = 789L;
        var searchTierMemorySize = 123L;
        var indexTierMemorySize = 456L;

        var searchActivity = new Activity(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);
        var indexActivity = new Activity(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);

        var searchMetrics = new SampledTierMetrics(searchTierMemorySize, searchActivity);
        var indexMetrics = new SampledTierMetrics(indexTierMemorySize, indexActivity);
        setMetricsServiceData(metricsService, searchMetrics, indexMetrics);

        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(
            metricsService,
            Duration.ofMinutes(15),
            buildSpMinTestProvider(spMinProvisionedMemory)
        );

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(metricValues.orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent));
        assertThat(metrics, hasSize(2));

        var metric1 = metrics.stream()
            .filter(m -> m.type().equals("es_vcu"))
            .filter(m -> m.id().equals("vcu:search"))
            .findFirst()
            .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);
        var metric2 = metrics.stream()
            .filter(m -> m.type().equals("es_vcu"))
            .filter(m -> m.id().equals("vcu:index"))
            .findFirst()
            .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);

        assertThat(metric1.id(), equalTo(SampledVCUMetricsProvider.VCU_METRIC_ID_PREFIX + ":search"));
        assertThat(metric1.sourceMetadata(), equalTo(Map.of()));
        assertThat(
            metric1.usageMetadata(),
            equalTo(
                Map.of(
                    USAGE_METADATA_APPLICATION_TIER,
                    "search",
                    USAGE_METADATA_ACTIVE,
                    "false",
                    USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY,
                    String.valueOf(spMinProvisionedMemory),
                    USAGE_METADATA_SP_MIN,
                    String.valueOf(DEFAULT_SP_MIN)
                )
            )
        );
        assertThat(metric1.value(), is(searchTierMemorySize));
        assertThat(metric1.meteredObjectCreationTime(), nullValue());

        assertThat(metric2.id(), equalTo(SampledVCUMetricsProvider.VCU_METRIC_ID_PREFIX + ":index"));
        assertThat(metric2.sourceMetadata(), equalTo(Map.of()));
        assertThat(metric2.usageMetadata(), equalTo(Map.of(USAGE_METADATA_APPLICATION_TIER, "index", USAGE_METADATA_ACTIVE, "false")));
        assertThat(metric2.value(), is(indexTierMemorySize));
        assertThat(metric1.meteredObjectCreationTime(), nullValue());
    }

    public void testGetMetrics() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var spMinProvisionedMemory = 789L;
        var searchTierMemorySize = 123L;
        var indexTierMemorySize = 456L;

        var now = Instant.now();
        // search activity is still active
        var searchActivity = new Activity(
            now.minus(Duration.ofMinutes(5)),
            now.minus(Duration.ofMinutes(100)),
            Instant.EPOCH,
            Instant.EPOCH
        );

        // index activity is not active since older than 15 minutes
        var indexActivity = new Activity(
            now.minus(Duration.ofMinutes(20)),
            now.minus(Duration.ofMinutes(100)),
            Instant.EPOCH,
            Instant.EPOCH
        );

        var searchMetrics = new SampledTierMetrics(searchTierMemorySize, searchActivity);
        var indexMetrics = new SampledTierMetrics(indexTierMemorySize, indexActivity);
        setMetricsServiceData(metricsService, searchMetrics, indexMetrics);

        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(
            metricsService,
            Duration.ofMinutes(15),
            buildSpMinTestProvider(spMinProvisionedMemory)
        );

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(metricValues.orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent));
        assertThat(metrics, hasSize(2));

        var metric1 = metrics.stream()
            .filter(m -> m.type().equals("es_vcu"))
            .filter(m -> m.id().equals("vcu:search"))
            .findFirst()
            .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);
        var metric2 = metrics.stream()
            .filter(m -> m.type().equals("es_vcu"))
            .filter(m -> m.id().equals("vcu:index"))
            .findFirst()
            .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);

        assertThat(metric1.id(), equalTo(SampledVCUMetricsProvider.VCU_METRIC_ID_PREFIX + ":search"));
        assertThat(metric1.sourceMetadata(), equalTo(Map.of()));
        assertThat(
            metric1.usageMetadata(),
            equalTo(
                Map.of(
                    USAGE_METADATA_APPLICATION_TIER,
                    "search",
                    USAGE_METADATA_ACTIVE,
                    "true",
                    USAGE_METADATA_LATEST_ACTIVITY_TIME,
                    searchActivity.lastActivityRecentPeriod().toString(),
                    USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY,
                    String.valueOf(spMinProvisionedMemory),
                    USAGE_METADATA_SP_MIN,
                    String.valueOf(DEFAULT_SP_MIN)
                )
            )
        );
        assertThat(metric1.value(), is(searchTierMemorySize));
        assertThat(metric1.meteredObjectCreationTime(), nullValue());

        assertThat(metric2.id(), equalTo(SampledVCUMetricsProvider.VCU_METRIC_ID_PREFIX + ":index"));
        assertThat(metric2.sourceMetadata(), equalTo(Map.of()));
        assertThat(
            metric2.usageMetadata(),
            equalTo(
                Map.of(
                    USAGE_METADATA_APPLICATION_TIER,
                    "index",
                    USAGE_METADATA_ACTIVE,
                    "false",
                    USAGE_METADATA_LATEST_ACTIVITY_TIME,
                    indexActivity.lastActivityRecentPeriod().toString()
                )
            )
        );
        assertThat(metric2.value(), is(indexTierMemorySize));
        assertThat(metric1.meteredObjectCreationTime(), nullValue());
    }

    public void testDifferentSpMinValues() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setMetricsServiceData(
            metricsService,
            SampledClusterMetricsServiceTests.randomSampledTierMetrics(),
            SampledClusterMetricsServiceTests.randomSampledTierMetrics()
        );

        long firstSpMin = randomIntBetween(0, 100);
        long secondSpMin = randomIntBetween(0, 100);
        var spMinProvider = buildSpMinTestProvider(List.of(new SPMinInfo(1, firstSpMin), new SPMinInfo(2, secondSpMin)));
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(metricsService, Duration.ofMinutes(15), spMinProvider);

        {
            var metricValues = sampledVCUMetricsProvider.getMetrics();
            Collection<MetricValue> metrics = iterableToList(
                metricValues.orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent)
            );
            var metric = metrics.stream()
                .filter(m -> m.id().equals("vcu:search"))
                .findFirst()
                .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);
            assertThat(metric.usageMetadata(), hasEntry(USAGE_METADATA_SP_MIN, String.valueOf(firstSpMin)));
        }

        {
            var metricValues = sampledVCUMetricsProvider.getMetrics();
            Collection<MetricValue> metrics = iterableToList(
                metricValues.orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent)
            );
            var metric = metrics.stream()
                .filter(m -> m.id().equals("vcu:search"))
                .findFirst()
                .orElseThrow(SampledVCUMetricsProviderTests::elementMustBePresent);
            assertThat(metric.usageMetadata(), hasEntry(USAGE_METADATA_SP_MIN, String.valueOf(secondSpMin)));
        }
    }

    public void testNoPersistentTaskNode() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(metricsService, Duration.ofMinutes(15), buildSpMinTestProvider());
        metricsService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.NO_NODE;

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    public void testAnotherNodeIsPersistentTaskNode() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(metricsService, Duration.ofMinutes(15), buildSpMinTestProvider());
        metricsService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.ANOTHER_NODE;

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    public void testThisNodeIsPersistentTaskNodeButNotReady() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(metricsService, Duration.ofMinutes(15), buildSpMinTestProvider());
        metricsService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    public void testSpMinProvisionedMemoryBadValue() {
        var provider = SPMinProvisionedMemoryProvider.build(clusterService, () -> randomNonNegativeLong(), () -> randomLongBetween(-5, 0));
        assertThat(provider.apply(null), nullValue());
    }

    public void testSpMinProvisionedMemoryStorageBadValue() {
        var provider = SPMinProvisionedMemoryProvider.build(clusterService, () -> randomLongBetween(-5, 0), () -> randomNonNegativeLong());
        assertThat(provider.apply(null), nullValue());
    }

    public void testSpMinProvisionedMemoryNotSearchNode() {
        var clusterService = mockClusterService(false);
        var provider = SPMinProvisionedMemoryProvider.build(clusterService, (NodeEnvironment) null);
        assertThat(provider.apply(null), nullValue());
    }

    public void testSpMinProvisionedMemoryNoShardsReturn0() {
        var clusterService = mockClusterService(true);
        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, Map.of(), Set.of());
        var provider = new SPMinProvisionedMemoryProvider(clusterService, 10, 10);
        assertThat(provider.apply(current).provisionedMemory(), equalTo(0L));
    }

    private static SampledClusterMetricsService.ShardSample randomShardSample(long interactiveSizeInBytes, long totalSizeInBytes) {
        var nonInteractiveSizeInBytes = totalSizeInBytes - interactiveSizeInBytes;
        return new SampledClusterMetricsService.ShardSample(
            randomUUID(),
            new ShardInfoMetrics(0, interactiveSizeInBytes, nonInteractiveSizeInBytes, 0, 0, 0, 0)
        );
    }

    // TODO Should we remove this behavior and this test?
    public void testSpMinProvisionedMemoryNegativeBoost() {
        // spMin = 1, interactiveSize = 100, totalSize = 100, provisionedStorage = 1000, provisionedRam = 1000
        // storageRamRatio = 1, since provisionedStorage and provisionedRam are equal
        // basePower = 5, since spMin=1 falls back to MINIMUM_SEARCH_POWER
        // boostPower = -4, spMin - basePower
        // cacheSize = 100, boostSize * boostPower + totalSize * basePower, 100 * -4 + 100 * 5
        assertThat(getSPMinProvisionedMemory(1, 100, 100, 1000, 1000), equalTo(100L));
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
        assertThat(getSPMinProvisionedMemory(null, 100, 200, 1000, 1000), equalTo(10500L));
    }

    public void testSpMinProvisionedMemoryStorageRatioD() {
        // spMin = 100, interactiveSize = 100, totalSize = 200, provisionedStorage = 2000, provisionedRam = 1000
        // storageRamRatio = 2, 2000/1000
        // basePower = 5, since default SPMin is 100
        // boostPower = 95, SPMin - basePower
        // cacheSize = 10500, 100 * 95 + 200 * 5
        // result = 10500 / 2
        assertThat(getSPMinProvisionedMemory(null, 100, 200, 2000, 1000), equalTo(5250L));
    }

    public void testSpMinProvisionedMemoryLotsOfShards() {
        var clusterService = mockClusterService(true);

        long totalInteractive = 0;
        long totalNonInteractive = 0;
        int numShards = between(1, 100);
        Map<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> shardSamples = new HashMap<>();
        for (int i = 0; i < numShards; ++i) {
            long interactiveSize = between(1, 10_000);
            long nonInteractiveSize = between(1, 10_000);
            totalInteractive += interactiveSize;
            totalNonInteractive += nonInteractiveSize;
            shardSamples.put(randomShardKey(), randomShardSample(interactiveSize, interactiveSize + nonInteractiveSize));
        }

        var current = new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardSamples, Set.of());

        long provisionedRam = between(1, 1000);
        long storageRamRatio = between(1, 10);
        long provisionedStorage = storageRamRatio * provisionedRam;
        var provider = new SPMinProvisionedMemoryProvider(clusterService, provisionedStorage, provisionedRam);

        // basePower = 5, since default SPMin is 100
        // boostPower = 95, SPMin - basePower
        // cacheSize = 10500, boostSize * boostPower + totalSize * basePower, 100 * 95 + 200 * 5
        long cacheSize = totalInteractive * 95 + (totalNonInteractive + totalInteractive) * 5;
        long expectSPMinRam = cacheSize / storageRamRatio;
        assertThat(provider.apply(current).provisionedMemory(), equalTo(expectSPMinRam));
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
        var provider = new SPMinProvisionedMemoryProvider(clusterService, provisionedStorage, provisionedRam);
        return provider.apply(current).provisionedMemory();
    }

    private static Function<SampledClusterMetrics, SPMinInfo> buildSpMinTestProvider(Long... provisionedMem) {
        var spMinInfos = Arrays.stream(provisionedMem).map(v -> new SPMinInfo(v, DEFAULT_SP_MIN)).toList();
        return buildSpMinTestProvider(spMinInfos);
    }

    private static Function<SampledClusterMetrics, SPMinInfo> buildSpMinTestProvider(List<SPMinInfo> spMinInfos) {
        var iter = spMinInfos.iterator();
        return current -> {
            assert iter.hasNext() : "Cannot call provider with more than number of provided values";
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
}
