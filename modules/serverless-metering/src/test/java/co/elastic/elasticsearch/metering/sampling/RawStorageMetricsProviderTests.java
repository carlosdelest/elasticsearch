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
import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.metering.TestUtils.hasBackfillStrategy;
import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.RawStorageMetricsProvider.RA_S_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.RawStorageMetricsProvider.RA_S_METRIC_TYPE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RawStorageMetricsProviderTests extends ESTestCase {

    static final Supplier<AssertionError> elementMustBePresent = () -> new AssertionError("Element must be present");

    private ClusterService clusterService;
    private SystemIndices systemIndices;
    private SampledClusterMetricsService metricsService;
    // FIXME
    private RawStorageMetricsProvider sampledStorageMetricsProvider;

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
        sampledStorageMetricsProvider = new RawStorageMetricsProvider(metricsService, clusterService, systemIndices);
    }

    private void setMetricsServiceState(Instant lastSearchActivity, ShardId shard, ShardInfoMetrics sample, SamplingStatus... flags) {
        setMetricsServiceState(lastSearchActivity, Map.of(shard, sample), flags);
    }

    private void setMetricsServiceState(Instant lastSearchActivity, Map<ShardId, ShardInfoMetrics> data, SamplingStatus... flags) {
        var searchTier = new SampledTierMetrics(0L, new Activity(lastSearchActivity, lastSearchActivity, Instant.EPOCH, Instant.EPOCH));
        metricsService.metricsState.set(
            new SamplingState(THIS_NODE, new SampledClusterMetrics(searchTier, SampledTierMetrics.EMPTY, data, Set.of(flags)))
        );
    }

    public void testGetMetrics() {
        var isActive = randomBoolean();
        var isHidden = randomBoolean();
        var shard1 = new ShardId(new Index("myIndex", "uuid"), 0);

        setMetadataIndicesLookup(mockedIndex(shard1.getIndexName(), false, isHidden));
        setMetricsServiceState(
            isActive ? Instant.now() : Instant.EPOCH,
            shard1,
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                .withData(110L, 11L, 0L, 11L)
                .withGeneration(1, 1, Instant.now().toEpochMilli())
                .build()
        );

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(1L)));

        var indexRAS = metrics.iterator().next();
        assertThat(indexRAS.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + shard1.getIndexName()));
        assertThat(
            indexRAS.sourceMetadata(),
            allOf(
                hasEntry("index", shard1.getIndexName()),
                hasEntry("system_index", "false"),
                hasEntry("hidden_index", Boolean.toString(isHidden))
            )
        );
        assertThat(indexRAS.value(), is(11L));
    }

    public void testSystemMetrics() {
        var shard1 = new ShardId(new Index("mySystemIndex", "uuid"), 0);

        when(systemIndices.isSystemIndex(shard1.getIndexName())).thenReturn(true);
        setMetricsServiceState(
            Instant.now(),
            shard1,
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                .withData(110L, 11L, 0L, 11L)
                .withGeneration(1, 1, Instant.now().toEpochMilli())
                .build()
        );

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(1L)));

        var indexRAS = metrics.iterator().next();
        assertThat(indexRAS.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + shard1.getIndexName()));
        assertThat(indexRAS.sourceMetadata(), allOf(hasEntry("index", shard1.getIndexName()), hasEntry("system_index", "true")));
        assertThat(indexRAS.value(), is(11L));
    }

    public void testGetMetricsWithNoSize() {
        var shard1Id = new ShardId(new Index("myIndex", "uuid"), 0);
        Instant creationDate = Instant.now();

        setMetricsServiceState(
            Instant.now(),
            shard1Id,
            ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                .withData(110L, 0, 0L, 0)
                .withGeneration(1, 1, creationDate.toEpochMilli())
                .build()
        );

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(metricValues.orElseThrow(elementMustBePresent));
        assertThat(metrics, empty());
    }

    public void testMultipleShardsWithMixedSizeType() {
        String indexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new ShardId(new Index(indexName, "uuid"), id);
            var size = 10L + id;
            var hasIngestSize = id < 5;
            return entry(
                shardId,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                    .withGeneration(1, 1, creationDate.toEpochMilli())
                    .withRawStats(id, 10L, id, 100L, 20L, 11L, 3, 3, 3 * id, 9 * id)
                    .build()
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(1L)));

        var rasMetrics = metrics.iterator().next();
        assertThat(rasMetrics.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(rasMetrics.sourceMetadata(), hasEntry("index", indexName));
        assertThat(
            rasMetrics.usageMetadata(),
            allOf(
                hasEntry("ra_size_segment_count", "45"),
                hasEntry("ra_size_segment_min_ra_avg", "3"),
                hasEntry("ra_size_segment_max_ra_avg", "3"),
                hasEntry("ra_size_segment_avg_ra_avg", "3"),
                hasEntry("ra_size_segment_stddev_ra_avg", "0"),
                hasEntry("ra_size_doc_count", "1000"),
                hasEntry("ra_size_deleted_doc_count", "200"),
                hasEntry("ra_size_approximated_doc_count", "110")
            )
        );
        assertThat(rasMetrics.value(), is(60L));
    }

    public void testMultipleIndicesWithMixedSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<ShardId, ShardInfoMetrics>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new ShardId(new Index(indexName, "uuid"), shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = indexIdx < 2;
                shardsInfo.put(
                    shardId,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .withRawStats(10, 10L, 8, 100L, 20L, 11L, 3, 10, 31, 163)
                        .build()
                );
            }
        }

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(2L)));

        for (MetricValue metric : metrics) {
            assertThat(
                metric.id(),
                oneOf(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 0, RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 1)
            );
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), oneOf(baseIndexName + 0, baseIndexName + 1)));
            assertThat(
                metric.usageMetadata(),
                allOf(
                    hasEntry(is("ra_size_segment_count"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_segment_min_ra_avg"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_segment_max_ra_avg"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_segment_avg_ra_avg"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_segment_stddev_ra_avg"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_doc_count"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_deleted_doc_count"), matchAsLong(greaterThan(0L))),
                    hasEntry(is("ra_size_approximated_doc_count"), matchAsLong(greaterThan(0L)))
                )
            );
            assertThat(metric.value(), is(145L));
        }
    }

    private Matcher<String> matchAsLong(Matcher<Long> matcher) {
        return transformedMatch(Long::parseLong, matcher);
    }

    public void testMultipleIndicesWithMixedShardSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<ShardId, ShardInfoMetrics>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new ShardId(new Index(indexName, "uuid" + indexIdx), shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = shardIdx < 5;
                shardsInfo.put(
                    shardId,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, hasIngestSize ? size : 0)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                );
            }
        }

        setMetricsServiceState(Instant.now(), shardsInfo);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(5L)));

        for (MetricValue metric : metrics) {
            assertThat(metric.id(), startsWith(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), aMapWithSize(greaterThanOrEqualTo(1)));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.value(), is(60L));
        }
    }

    public void testFailedShards() {
        String indexName = "myMultiShardIndex";
        int failedShardId = 7;
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardKey = new ShardId(new Index(indexName, "uuid"), id);
            var size = failedShardId == id ? 0 : 10L + id;
            return entry(
                shardKey,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(110L, size, 0L, size)
                    .withGeneration(1, 1, creationDate.toEpochMilli())
                    .build()
            );
        }).toList();

        setMetricsServiceState(Instant.now(), Maps.ofEntries(shardsInfo), SamplingStatus.PARTIAL);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(1L)));
        for (MetricValue metric : metrics) {
            assertThat(metric.id(), startsWith(RA_S_METRIC_ID_PREFIX + ":" + indexName));
            assertThat(metric.type(), is(RA_S_METRIC_TYPE));
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
        var metricValues = sampledStorageMetricsProvider.getMetrics(); // empty initially
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
