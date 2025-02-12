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
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingState;
import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static co.elastic.elasticsearch.metering.TestUtils.hasBackfillStrategy;
import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.SPMinProvisionedMemoryCalculator.SPMinInfo;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_ACTIVE;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_LATEST_ACTIVITY_TIME;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SampledVCUMetricsProviderTests extends ESTestCase {
    private static final long DEFAULT_SP_MIN = 100;

    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
    }

    private void setMetricsServiceData(
        SampledClusterMetricsService metricsService,
        SampledTierMetrics searchTierMetrics,
        SampledTierMetrics indexTierMetrics
    ) {
        metricsService.metricsState.set(
            new SamplingState(THIS_NODE, new SampledClusterMetrics(searchTierMetrics, indexTierMetrics, Map.of(), Set.of()))
        );
    }

    private static AssertionError elementMustBePresent() {
        return new AssertionError("Element must be present");
    }

    public void testGetMetricsEmptyActivity() {

        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);

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
            buildSpMinTestProvider(new SPMinInfo(spMinProvisionedMemory, DEFAULT_SP_MIN, 1.0)),
            MeterRegistry.NOOP
        );

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(VCUSampledMetricsBackfillStrategy.class))));

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
                    String.valueOf(DEFAULT_SP_MIN),
                    USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO,
                    "1"
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
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);

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
            buildSpMinTestProvider(new SPMinInfo(spMinProvisionedMemory, DEFAULT_SP_MIN, 1.0)),
            MeterRegistry.NOOP
        );

        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(VCUSampledMetricsBackfillStrategy.class))));

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
                    String.valueOf(DEFAULT_SP_MIN),
                    USAGE_METADATA_SP_MIN_STORAGE_RAM_RATIO,
                    "1"
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
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setMetricsServiceData(
            metricsService,
            SampledClusterMetricsServiceTests.randomSampledTierMetrics(),
            SampledClusterMetricsServiceTests.randomSampledTierMetrics()
        );

        long firstSpMin = randomIntBetween(0, 100);
        long secondSpMin = randomIntBetween(0, 100);
        var spMinProvider = buildSpMinTestProvider(new SPMinInfo(1, firstSpMin, 0.0), new SPMinInfo(2, secondSpMin, 0.0));
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(
            metricsService,
            Duration.ofMinutes(15),
            spMinProvider,
            MeterRegistry.NOOP
        );

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

    public void testEmptyMetrics() {
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE); // empty initially
        var spMinProvisionedMemoryCalculator = mock(SPMinProvisionedMemoryCalculator.class);
        var sampledVCUMetricsProvider = new SampledVCUMetricsProvider(
            metricsService,
            Duration.ofMinutes(15),
            spMinProvisionedMemoryCalculator,
            MeterRegistry.NOOP
        );
        var metricValues = sampledVCUMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
        verifyNoInteractions(spMinProvisionedMemoryCalculator);
    }

    private static SPMinProvisionedMemoryCalculator buildSpMinTestProvider(SPMinInfo spMinInfo, SPMinInfo... spMinInfos) {
        var mock = mock(SPMinProvisionedMemoryCalculator.class);
        when(mock.calculate(any())).thenReturn(spMinInfo, spMinInfos);
        return mock;
    }
}
