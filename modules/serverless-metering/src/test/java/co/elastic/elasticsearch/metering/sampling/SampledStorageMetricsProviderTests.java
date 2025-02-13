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
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingState;
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider.MetricValues;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
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
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.IX_INDEX_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.IX_METRIC_TYPE;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.IX_SHARD_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.RA_S_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.RA_S_METRIC_TYPE;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledStorageMetricsProviderTests extends ESTestCase {

    static final Predicate<MetricValue> isIxShardMetric = m -> m.type().equals(IX_METRIC_TYPE)
        && m.id().startsWith(IX_SHARD_METRIC_ID_PREFIX);
    static final Predicate<MetricValue> isIxIndexMetric = m -> m.type().equals(IX_METRIC_TYPE)
        && m.id().startsWith(IX_INDEX_METRIC_ID_PREFIX);
    static final Predicate<MetricValue> isRasMetric = m -> m.type().equals(RA_S_METRIC_TYPE);
    static final Supplier<AssertionError> elementMustBePresent = () -> new AssertionError("Element must be present");

    private ClusterService clusterService;
    private SystemIndices systemIndices;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE.copyAndUpdate(b -> b.metadata(mock(Metadata.class))));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        systemIndices = mock(SystemIndices.class);
    }

    private void setInternalIndexInfoServiceData(
        SampledClusterMetricsService indexInfoService,
        Map<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> data
    ) {
        setInternalIndexInfoServiceData(indexInfoService, data, Set.of());
    }

    private void setInternalIndexInfoServiceData(
        SampledClusterMetricsService indexInfoService,
        Map<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> data,
        Set<SampledClusterMetricsService.SamplingStatus> flags
    ) {
        indexInfoService.metricsState.set(
            new SamplingState(THIS_NODE, new SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, data, flags))
        );
    }

    private MetricValue findFirstMatch(MetricValues metrics, Predicate<MetricValue> predicate) {
        return StreamSupport.stream(metrics.spliterator(), false).filter(predicate).findFirst().orElseThrow(elementMustBePresent);
    }

    private List<MetricValue> findAllMetrics(MetricValues metrics, Predicate<MetricValue> predicate) {
        return StreamSupport.stream(metrics.spliterator(), false).filter(predicate).toList();
    }

    public void testGetMetrics() {
        String indexName = "myIndex";
        boolean isHidden = randomBoolean();
        int shardIdInt = 0;
        var shard1Id = new SampledClusterMetricsService.ShardKey(indexName, shardIdInt);

        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                new SampledClusterMetricsService.ShardSample(
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, 11L, 0L, 11L)
                        .withGeneration(1, 1, Instant.now().toEpochMilli())
                        .build()
                )
            )
        );

        Metadata metadata = clusterService.state().metadata();
        mockedMetadata(metadata, mockedIndex(indexName, false, isHidden));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(3L)));

        var shardIX = findFirstMatch(metrics, isIxShardMetric);
        var indexIX = findFirstMatch(metrics, isIxIndexMetric);
        var indexRAS = findFirstMatch(metrics, isRasMetric);

        assertThat(shardIX.id(), equalTo(IX_SHARD_METRIC_ID_PREFIX + ":" + indexName + ":" + shardIdInt));
        assertThat(
            shardIX.sourceMetadata(),
            allOf(
                hasEntry("index", indexName),
                hasEntry("system_index", "false"),
                hasEntry("shard", "" + shardIdInt),
                hasEntry("hidden_index", Boolean.toString(isHidden))
            )
        );
        assertThat(shardIX.value(), is(11L));
        assertThat(shardIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexIX.id(), equalTo(IX_INDEX_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(
            indexIX.sourceMetadata(),
            allOf(hasEntry("index", indexName), hasEntry("system_index", "false"), hasEntry("hidden_index", Boolean.toString(isHidden)))
        );
        assertThat(indexIX.value(), is(11L));
        assertThat(indexIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexRAS.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(
            indexRAS.sourceMetadata(),
            allOf(hasEntry("index", indexName), hasEntry("system_index", "false"), hasEntry("hidden_index", Boolean.toString(isHidden)))
        );
        assertThat(indexRAS.value(), is(11L));
    }

    public void testSystemMetrics() {
        String indexName = "mySystemIndex";
        int shardIdInt = 0;
        var shard1Id = new SampledClusterMetricsService.ShardKey(indexName, shardIdInt);

        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                new SampledClusterMetricsService.ShardSample(
                    indexName,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, 11L, 0L, 11L)
                        .withGeneration(1, 1, Instant.now().toEpochMilli())
                        .build()
                )
            )
        );

        when(systemIndices.isSystemIndex(indexName)).thenReturn(true);

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(3L)));

        var shardIX = findFirstMatch(metrics, isIxShardMetric);
        var indexIX = findFirstMatch(metrics, isIxIndexMetric);
        var indexRAS = findFirstMatch(metrics, isRasMetric);

        assertThat(shardIX.id(), equalTo(IX_SHARD_METRIC_ID_PREFIX + ":" + indexName + ":" + shardIdInt));
        assertThat(
            shardIX.sourceMetadata(),
            allOf(hasEntry("index", indexName), hasEntry("system_index", "true"), hasEntry("shard", "" + shardIdInt))
        );
        assertThat(shardIX.value(), is(11L));
        assertThat(shardIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexIX.id(), equalTo(IX_INDEX_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(indexIX.sourceMetadata(), allOf(hasEntry("index", indexName), hasEntry("system_index", "true")));
        assertThat(indexIX.value(), is(11L));
        assertThat(indexIX.meteredObjectCreationTime(), greaterThan(Instant.EPOCH));

        assertThat(indexRAS.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(indexRAS.sourceMetadata(), allOf(hasEntry("index", indexName), hasEntry("system_index", "true")));
        assertThat(indexRAS.value(), is(11L));
    }

    public void testGetMetricsWithNoRasSize() {
        String indexName = "myIndex";
        int shardIdInt = 0;
        var shard1Id = new SampledClusterMetricsService.ShardKey(indexName, shardIdInt);
        Instant creationDate = Instant.now();
        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                new SampledClusterMetricsService.ShardSample(
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, 11L, 0L, 0)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metricValues = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metricValues, not(hasItem(transformedMatch(MetricValue::type, equalTo(RA_S_METRIC_TYPE)))));
    }

    public void testGetMetricsWithNoIXSize() {
        String indexName = "myIndex";
        int shardIdInt = 0;
        var shard1Id = new SampledClusterMetricsService.ShardKey(indexName, shardIdInt);
        Instant creationDate = Instant.now();
        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                new SampledClusterMetricsService.ShardSample(
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, 0, 0L, 11L)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metricValues = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metricValues, not(hasItem(transformedMatch(MetricValue::type, equalTo(IX_METRIC_TYPE)))));
    }

    public void testGetMetricsWithNoSize() {
        String indexName = "myIndex";
        int shardIdInt = 0;
        var shard1Id = new SampledClusterMetricsService.ShardKey(indexName, shardIdInt);
        Instant creationDate = Instant.now();
        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                new SampledClusterMetricsService.ShardSample(
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, 0, 0L, 0)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(metricValues.orElseThrow(elementMustBePresent));
        assertThat(metrics, empty());
    }

    public void testMultipleShardsWithMixedSizeType() {
        String indexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new SampledClusterMetricsService.ShardKey(indexName, id);
            var size = 10L + id;
            var hasIngestSize = id < 5;
            return entry(
                shardId,
                new SampledClusterMetricsService.ShardSample(
                    indexName,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .withRAStats(id, 10L, id, 100L, 20L, 11L, 3, 3, 3 * id, 9 * id)
                        .build()
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(12L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);
        var rasMetrics = findAllMetrics(metrics, isRasMetric);

        assertThat(ixShardMetrics, hasSize(10));
        assertThat(ixIndexMetrics, hasSize(1));
        assertThat(rasMetrics, hasSize(1));

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

        for (MetricValue metric : rasMetrics) {
            assertThat(metric.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
            assertThat(metric.sourceMetadata(), hasEntry("index", indexName));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_segment_count", "45"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_segment_min_ra_avg", "3"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_segment_max_ra_avg", "3"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_segment_avg_ra_avg", "3"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_segment_stddev_ra_avg", "0"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_doc_count", "1000"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_deleted_doc_count", "200"));
            assertThat(metric.usageMetadata(), hasEntry("ra_size_approximated_doc_count", "110"));
            assertThat(metric.value(), is(60L));
        }
    }

    public void testMultipleIndicesWithMixedSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new SampledClusterMetricsService.ShardKey(indexName, shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = indexIdx < 2;
                shardsInfo.put(
                    shardId,
                    new SampledClusterMetricsService.ShardSample(
                        indexName,
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .withRAStats(10, 10L, 8, 100L, 20L, 11L, 3, 10, 31, 163)
                            .build()
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(57L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);
        var rasMetrics = findAllMetrics(metrics, isRasMetric);

        assertThat(ixShardMetrics, hasSize(50));
        assertThat(ixIndexMetrics, hasSize(5));
        assertThat(rasMetrics, hasSize(2));

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

        for (MetricValue metric : rasMetrics) {
            assertThat(
                metric.id(),
                oneOf(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 0, RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 1)
            );
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), oneOf(baseIndexName + 0, baseIndexName + 1)));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_segment_count"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_segment_min_ra_avg"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_segment_max_ra_avg"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_segment_avg_ra_avg"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_segment_stddev_ra_avg"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_doc_count"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_deleted_doc_count"), matchAsLong(greaterThan(0L))));
            assertThat(metric.usageMetadata(), hasEntry(is("ra_size_approximated_doc_count"), matchAsLong(greaterThan(0L))));
            assertThat(metric.value(), is(145L));
        }
    }

    private Matcher<String> matchAsLong(Matcher<Long> matcher) {
        return transformedMatch(Long::parseLong, matcher);
    }

    public void testMultipleIndicesWithMixedShardSizeType() {
        String baseIndexName = "myMultiShardIndex";
        Instant creationDate = Instant.now();
        var shardsInfo = new LinkedHashMap<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample>();
        for (var indexIdx = 0; indexIdx < 5; indexIdx++) {
            var indexName = baseIndexName + indexIdx;
            for (var shardIdx = 0; shardIdx < 10; shardIdx++) {
                var shardId = new SampledClusterMetricsService.ShardKey(indexName, shardIdx);
                var size = 10L + shardIdx;
                var hasIngestSize = shardIdx < 5;
                shardsInfo.put(
                    shardId,
                    new SampledClusterMetricsService.ShardSample(
                        indexName,
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .build()
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);

        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));
        assertThat(metrics, transformedMatch(Iterables::size, is(60L)));

        var ixShardMetrics = findAllMetrics(metrics, isIxShardMetric);
        var ixIndexMetrics = findAllMetrics(metrics, isIxIndexMetric);
        var rasMetrics = findAllMetrics(metrics, isRasMetric);

        assertThat(ixShardMetrics, hasSize(50));
        assertThat(ixIndexMetrics, hasSize(5));
        assertThat(rasMetrics, hasSize(5));

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

        for (MetricValue metric : rasMetrics) {
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
            var shardKey = new SampledClusterMetricsService.ShardKey(indexName, id);
            var size = failedShardId == id ? 0 : 10L + id;
            return entry(
                shardKey,
                new SampledClusterMetricsService.ShardSample(
                    indexName,
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, size)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo, Set.of(SampledClusterMetricsService.SamplingStatus.PARTIAL));

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider(systemIndices);

        var metrics = sampledStorageMetricsProvider.getMetrics().orElseThrow(elementMustBePresent);
        assertThat(metrics, hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class)));

        assertThat(metrics, transformedMatch(Iterables::size, is(11L)));
        for (MetricValue metric : metrics) {
            assertThat(
                metric.id(),
                anyOf(
                    startsWith(IX_SHARD_METRIC_ID_PREFIX + ":" + indexName),
                    startsWith(IX_INDEX_METRIC_ID_PREFIX + ":" + indexName),
                    startsWith(RA_S_METRIC_ID_PREFIX + ":" + indexName)
                )
            );
            assertThat(metric.type(), is(either(equalTo(IX_METRIC_TYPE)).or(equalTo(RA_S_METRIC_TYPE))));
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
        var metricsService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP, THIS_NODE); // empty initially
        var sampledStorageMetricsProvider = metricsService.createSampledStorageMetricsProvider(systemIndices);

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    private static Metadata mockedMetadata(Metadata mock, IndexAbstraction... indices) {
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>(
            Arrays.stream(indices).collect(Collectors.toMap(IndexAbstraction::getName, identity()))
        );
        when(mock.getIndicesLookup()).thenReturn(lookup);
        return mock;
    }

    private static IndexAbstraction mockedIndex(String name, boolean isSystem, boolean isHidden) {
        IndexAbstraction index = mock();
        when(index.getName()).thenReturn(name);
        when(index.isHidden()).thenReturn(isHidden);
        when(index.isSystem()).thenReturn(isSystem);
        return index;
    }
}
