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
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingState;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingStatus;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.ShardKey;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.ShardSample;
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider.MetricValues;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static co.elastic.elasticsearch.metering.TestUtils.hasBackfillStrategy;
import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.IndexSizeMetricsProvider.IX_INDEX_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.IndexSizeMetricsProvider.IX_METRIC_TYPE;
import static co.elastic.elasticsearch.metering.sampling.IndexSizeMetricsProvider.IX_SHARD_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexSizeMetricsProviderTests extends ESTestCase {

    static final Predicate<MetricValue> isIxShardMetric = m -> m.type().equals(IX_METRIC_TYPE)
        && m.id().startsWith(IX_SHARD_METRIC_ID_PREFIX);
    static final Predicate<MetricValue> isIxIndexMetric = m -> m.type().equals(IX_METRIC_TYPE)
        && m.id().startsWith(IX_INDEX_METRIC_ID_PREFIX);
    static final Supplier<AssertionError> elementMustBePresent = () -> new AssertionError("Element must be present");

    private ClusterService clusterService;
    private SystemIndices systemIndices;
    private SampledClusterMetricsService metricsService;
    private IndexSizeMetricsProvider indexSizeMetricsProvider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        final ClusterState clusterState = ClusterState.EMPTY_STATE.copyAndUpdate(b -> {
            final ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
            when(projectMetadata.id()).thenReturn(Metadata.DEFAULT_PROJECT_ID);
            final Metadata metadata = mock(Metadata.class);
            when(metadata.projects()).thenReturn(Map.of(Metadata.DEFAULT_PROJECT_ID, projectMetadata));
            when(metadata.getProject()).thenReturn(projectMetadata);
            b.metadata(metadata);
        });
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        systemIndices = mock(SystemIndices.class);
        metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        indexSizeMetricsProvider = new IndexSizeMetricsProvider(metricsService, clusterService, systemIndices);
    }

    private void setMetricsServiceState(Instant lastSearchActivity, ShardKey shard, ShardSample sample, SamplingStatus... flags) {
        setMetricsServiceState(lastSearchActivity, Map.of(shard, sample), flags);
    }

    private void setMetricsServiceState(Instant lastSearchActivity, Map<ShardKey, ShardSample> data, SamplingStatus... flags) {
        var searchTier = new SampledTierMetrics(0L, new Activity(lastSearchActivity, lastSearchActivity, Instant.EPOCH, Instant.EPOCH));
        metricsService.metricsState.set(
            new SamplingState(THIS_NODE, new SampledClusterMetrics(searchTier, SampledTierMetrics.EMPTY, data, Set.of(flags)))
        );
    }

    private MetricValue findFirstMatch(MetricValues metrics, Predicate<MetricValue> predicate) {
        return StreamSupport.stream(metrics.spliterator(), false).filter(predicate).findFirst().orElseThrow(elementMustBePresent);
    }

    private List<MetricValue> findAllMetrics(MetricValues metrics, Predicate<MetricValue> predicate) {
        return StreamSupport.stream(metrics.spliterator(), false).filter(predicate).toList();
    }

    public void testGetMetrics() {
        var isActive = randomBoolean();
        var isHidden = randomBoolean();
        var shard1 = new ShardKey("myIndex", 0);

        setMetadataIndicesLookup(mockedIndex(shard1.indexName(), false, isHidden));
        setMetricsServiceState(
            isActive ? Instant.now() : Instant.EPOCH,
            shard1,
            new ShardSample(
                "myIndexUUID",
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(110L, 11L, 0L, 11L)
                    .withGeneration(1, 1, Instant.now().toEpochMilli())
                    .build()
            )
        );
        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(2L)));

        var shardIX = findFirstMatch(metrics, isIxShardMetric);
        var indexIX = findFirstMatch(metrics, isIxIndexMetric);

        assertThat(shardIX.id(), equalTo(IX_SHARD_METRIC_ID_PREFIX + ":" + shard1));
        assertThat(
            shardIX.sourceMetadata(),
            allOf(
                hasEntry("index", shard1.indexName()),
                hasEntry("system_index", "false"),
                hasEntry("shard", "" + shard1.shardId()),
                hasEntry("hidden_index", Boolean.toString(isHidden))
            )
        );
        assertThat(shardIX.value(), is(11L));
        assertThat(shardIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexIX.id(), equalTo(IX_INDEX_METRIC_ID_PREFIX + ":" + shard1.indexName()));
        assertThat(
            indexIX.sourceMetadata(),
            allOf(
                hasEntry("index", shard1.indexName()),
                hasEntry("system_index", "false"),
                hasEntry("hidden_index", Boolean.toString(isHidden))
            )
        );
        assertThat(indexIX.value(), is(11L));
        assertThat(indexIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));
    }

    public void testSystemMetrics() {
        var shard1 = new ShardKey("mySystemIndex", 0);

        when(systemIndices.isSystemIndex(shard1.indexName())).thenReturn(true);

        setMetricsServiceState(
            Instant.now(),
            shard1,
            new ShardSample(
                "myIndexUUID",
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(110L, 11L, 0L, 11L)
                    .withGeneration(1, 1, Instant.now().toEpochMilli())
                    .build()
            )
        );

        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(2L)));

        var shardIX = findFirstMatch(metrics, isIxShardMetric);
        var indexIX = findFirstMatch(metrics, isIxIndexMetric);

        assertThat(shardIX.id(), equalTo(IX_SHARD_METRIC_ID_PREFIX + ":" + shard1));
        assertThat(
            shardIX.sourceMetadata(),
            allOf(hasEntry("index", shard1.indexName()), hasEntry("system_index", "true"), hasEntry("shard", "" + shard1.shardId()))
        );
        assertThat(shardIX.value(), is(11L));
        assertThat(shardIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexIX.id(), equalTo(IX_INDEX_METRIC_ID_PREFIX + ":" + shard1.indexName()));
        assertThat(indexIX.sourceMetadata(), allOf(hasEntry("index", shard1.indexName()), hasEntry("system_index", "true")));
        assertThat(indexIX.value(), is(11L));
        assertThat(indexIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));
    }

    public void testGetMetricsWithNoSize() {
        var shard1Id = new ShardKey("myIndex", 0);
        Instant creationDate = Instant.now();

        setMetricsServiceState(
            Instant.now(),
            shard1Id,
            new ShardSample(
                "myIndexUUID",
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(110L, 0, 0L, 0)
                    .withGeneration(1, 1, creationDate.toEpochMilli())
                    .build()
            )
        );

        var metricValues = indexSizeMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(metricValues.orElseThrow(elementMustBePresent));
        assertThat(metrics, empty());
    }

    public void testMultipleShardsWithMixedSizeType() {
        String indexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new ShardKey(indexName, id);
            var size = 10L + id;
            var hasIngestSize = id < 5;
            return entry(
                shardId,
                new ShardSample(
                    indexName,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .withRawStats(id, 10L, id, 100L, 20L, 11L, 3, 3, 3 * id, 9 * id)
                        .build()
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(11L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);

        assertThat(ixShardMetrics, hasSize(10));
        assertThat(ixIndexMetrics, hasSize(1));

        int shard = 0;
        for (MetricValue metric : ixShardMetrics) {
            assertThat(metric.id(), equalTo(IX_SHARD_METRIC_ID_PREFIX + ":" + indexName + ":" + shard));
            assertThat(metric.sourceMetadata(), allOf(hasEntry("index", indexName), hasEntry("shard", "" + shard)));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
            shard++;
        }

        for (MetricValue metric : ixIndexMetrics) {
            assertThat(metric.id(), equalTo(IX_INDEX_METRIC_ID_PREFIX + ":" + indexName));
            assertThat(metric.sourceMetadata(), hasEntry("index", indexName));
            assertThat(metric.sourceMetadata(), not(hasKey("shard")));
            assertThat(metric.value(), is(145L));
        }
    }

    public void testMultipleIndicesWithMixedSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<ShardKey, ShardSample>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new ShardKey(indexName, shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = indexIdx < 2;
                shardsInfo.put(
                    shardId,
                    new ShardSample(
                        indexName,
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .withRawStats(10, 10L, 8, 100L, 20L, 11L, 3, 10, 31, 163)
                            .build()
                    )
                );
            }
        }

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(55L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);

        assertThat(ixShardMetrics, hasSize(50));
        assertThat(ixIndexMetrics, hasSize(5));

        for (MetricValue metric : ixShardMetrics) {
            assertThat(metric.id(), startsWith(IX_SHARD_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        for (MetricValue metric : ixIndexMetrics) {
            assertThat(metric.id(), startsWith(IX_INDEX_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), not(hasKey("shard")));
            assertThat(metric.value(), is(145L));
        }
    }

    public void testMultipleIndicesWithMixedShardSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<ShardKey, ShardSample>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new ShardKey(indexName, shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = shardIdx < 5;
                shardsInfo.put(
                    shardId,
                    new ShardSample(
                        indexName,
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .build()
                    )
                );
            }
        }

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(55L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);

        assertThat(ixShardMetrics, hasSize(50));
        assertThat(ixIndexMetrics, hasSize(5));

        for (MetricValue metric : ixShardMetrics) {
            assertThat(metric.id(), startsWith(IX_SHARD_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        for (MetricValue metric : ixIndexMetrics) {
            assertThat(metric.id(), startsWith(IX_INDEX_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), not(hasKey("shard")));
            assertThat(metric.value(), is(145L));
        }
    }

    public void testFailedShards() {
        String indexName = "myMultiShardIndex";
        int failedShardId = 7;
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardKey = new ShardKey(indexName, id);
            var size = failedShardId == id ? 0 : 10L + id;
            return entry(
                shardKey,
                new ShardSample(
                    indexName,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, size)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            );
        }).toList();

        setMetricsServiceState(Instant.now(), Maps.ofEntries(shardsInfo), SamplingStatus.PARTIAL);

        var metrics = indexSizeMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(10L)));
        for (MetricValue metric : metrics) {
            assertThat(
                metric.id(),
                anyOf(startsWith(IX_SHARD_METRIC_ID_PREFIX + ":" + indexName), startsWith(IX_INDEX_METRIC_ID_PREFIX + ":" + indexName))
            );
            assertThat(metric.sourceMetadata(), hasEntry("index", indexName));
            assertThat(metric.sourceMetadata(), hasEntry("partial", "true"));

            var isFailed = metric.id().endsWith(":" + failedShardId);
            if (isFailed) {
                assertThat(metric.value(), is(0L));
            } else {
                assertThat(metric.value(), is(greaterThanOrEqualTo(10L)));
            }
        }
    }

    public void testEmptyMetrics() {
        var metricValues = indexSizeMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    private void setMetadataIndicesLookup(IndexAbstraction... indices) {
        Metadata mock = clusterService.state().metadata(); //
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>(
            Arrays.stream(indices).collect(Collectors.toMap(IndexAbstraction::getName, identity()))
        );
        when(mock.getProject().getIndicesLookup()).thenReturn(lookup);
    }

    private static IndexAbstraction mockedIndex(String name, boolean isSystem, boolean isHidden) {
        IndexAbstraction index = mock();
        when(index.getName()).thenReturn(name);
        when(index.isHidden()).thenReturn(isHidden);
        when(index.isSystem()).thenReturn(isSystem);
        return index;
    }
}
