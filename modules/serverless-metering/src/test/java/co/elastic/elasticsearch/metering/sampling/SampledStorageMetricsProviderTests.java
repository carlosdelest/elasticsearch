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
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.metering.TestUtils.hasBackfillStrategy;
import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.IX_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledStorageMetricsProvider.RA_S_METRIC_ID_PREFIX;
import static java.util.Map.entry;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledStorageMetricsProviderTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
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
        indexInfoService.collectedMetrics.set(
            new SampledClusterMetricsService.SampledClusterMetrics(SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, data, flags)
        );
        indexInfoService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
    }

    private static AssertionError elementMustBePresent() {
        return new AssertionError("Element must be present");
    }

    public void testGetMetrics() {
        String indexName = "myIndex";
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

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        assertThat(metrics, hasSize(2));

        var metric1 = metrics.stream()
            .filter(m -> m.type().equals("es_indexed_data"))
            .findFirst()
            .orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent);
        var metric2 = metrics.stream()
            .filter(m -> m.type().equals("es_raw_stored_data"))
            .findFirst()
            .orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent);

        assertThat(metric1.id(), equalTo(IX_METRIC_ID_PREFIX + ":" + indexName + ":" + shardIdInt));
        assertThat(metric1.sourceMetadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));
        assertThat(metric1.value(), is(11L));
        assertThat(metric1.meteredObjectCreationTime(), notNullValue());
        assertTrue(metric1.meteredObjectCreationTime().isAfter(Instant.EPOCH));

        assertThat(metric2.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(metric2.sourceMetadata(), hasEntry("index", indexName));
        assertThat(metric2.value(), is(11L));
    }

    public void testGetMetricsWithNoStoredIngestSize() {
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
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );
        assertThat(metrics, hasSize(1));

        var metric = (MetricValue) metrics.toArray()[0];
        assertThat(metric.id(), equalTo(IX_METRIC_ID_PREFIX + ":" + indexName + ":" + shardIdInt));
        assertThat(metric.type(), equalTo("es_indexed_data"));
        assertThat(metric.sourceMetadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));

        assertThat(metric.value(), is(11L));
    }

    public void testGetMetricsWithNoIndexSize() {
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
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );
        assertThat(metrics, hasSize(1));

        var metric = (MetricValue) metrics.toArray()[0];
        assertThat(metric.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(metric.type(), equalTo("es_raw_stored_data"));
        assertThat(metric.sourceMetadata(), hasEntry("index", indexName));

        assertThat(metric.value(), is(11L));
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
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );
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
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .withRAStats(id, 10L, id, 100L, 20L, 11L, 3, 3, 3 * id, 9 * id)
                        .build()
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        var indexedDataMetrics = metrics.stream().filter(x -> x.type().equals("es_indexed_data")).toList();
        var rawStoredDataMetrics = metrics.stream().filter(x -> x.type().equals("es_raw_stored_data")).toList();

        assertThat(metrics, hasSize(11));
        assertThat(indexedDataMetrics, hasSize(10));
        assertThat(rawStoredDataMetrics, hasSize(1));

        int shard = 0;
        for (MetricValue metric : indexedDataMetrics) {
            assertThat(metric.id(), equalTo(IX_METRIC_ID_PREFIX + ":" + indexName + ":" + shard));
            assertThat(metric.sourceMetadata(), equalTo(Map.of("index", indexName, "shard", "" + shard)));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
            shard++;
        }

        for (MetricValue metric : rawStoredDataMetrics) {
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
            shard++;
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
                        "myIndexUUID",
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0L)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .withRAStats(10, 10L, 8, 100L, 20L, 11L, 3, 10, 31, 163)
                            .build()
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        var indexedDataMetrics = metrics.stream().filter(x -> x.type().equals("es_indexed_data")).toList();
        var rawStoredDataMetrics = metrics.stream().filter(x -> x.type().equals("es_raw_stored_data")).toList();

        assertThat(metrics, hasSize(52));
        assertThat(indexedDataMetrics, hasSize(50));
        assertThat(rawStoredDataMetrics, hasSize(2));

        for (MetricValue metric : indexedDataMetrics) {
            assertThat(metric.id(), startsWith(IX_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), aMapWithSize(2));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        assertThat(
            rawStoredDataMetrics.stream().map(MetricValue::id).toList(),
            containsInAnyOrder(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 0, RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 1)
        );
        var metadataList = rawStoredDataMetrics.stream().map(MetricValue::sourceMetadata).toList();
        assertThat(metadataList, everyItem(aMapWithSize(greaterThanOrEqualTo(1))));
        assertThat(metadataList, everyItem(hasEntry(is("index"), startsWith(baseIndexName))));

        var usageMetadataList = rawStoredDataMetrics.stream().map(MetricValue::usageMetadata).toList();
        assertThat(usageMetadataList, everyItem(aMapWithSize(greaterThanOrEqualTo(11))));

        assertThat(usageMetadataList, everyItem(hasEntry(is("ra_size_segment_count"), transformedMatch(Long::parseLong, greaterThan(0L)))));
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_segment_min_ra_avg"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_segment_max_ra_avg"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_segment_avg_ra_avg"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_segment_stddev_ra_avg"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );
        assertThat(usageMetadataList, everyItem(hasEntry(is("ra_size_doc_count"), transformedMatch(Long::parseLong, greaterThan(0L)))));
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_deleted_doc_count"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );
        assertThat(
            usageMetadataList,
            everyItem(hasEntry(is("ra_size_approximated_doc_count"), transformedMatch(Long::parseLong, greaterThan(0L))))
        );

        assertThat(rawStoredDataMetrics.stream().map(MetricValue::value).toList(), everyItem(is(145L)));
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
                        "myIndexUUID",
                        ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                            .withData(110L, size, 0L, hasIngestSize ? size : 0)
                            .withGeneration(1, 1, creationDate.toEpochMilli())
                            .build()
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        var indexedDataMetrics = metrics.stream().filter(x -> x.type().equals("es_indexed_data")).toList();
        var rawStoredDataMetrics = metrics.stream().filter(x -> x.type().equals("es_raw_stored_data")).toList();

        assertThat(metrics, hasSize(55));
        assertThat(indexedDataMetrics, hasSize(50));
        assertThat(rawStoredDataMetrics, hasSize(5));

        for (MetricValue metric : indexedDataMetrics) {
            assertThat(metric.id(), startsWith(IX_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), aMapWithSize(2));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.sourceMetadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        for (MetricValue metric : rawStoredDataMetrics) {
            assertThat(metric.id(), startsWith(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.sourceMetadata(), aMapWithSize(greaterThanOrEqualTo(1)));
            assertThat(metric.sourceMetadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.value(), is(60L));
        }
    }

    public void testFailedShards() {
        String indexName = "myMultiShardIndex";
        int failedIndex = 7;
        Instant creationDate = Instant.now();
        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new SampledClusterMetricsService.ShardKey(indexName, id);
            var size = failedIndex == id ? 0 : 10L + id;
            return entry(
                shardId,
                new SampledClusterMetricsService.ShardSample(
                    "myIndexUUID",
                    ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                        .withData(110L, size, 0L, size)
                        .withGeneration(1, 1, creationDate.toEpochMilli())
                        .build()
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo, Set.of(SampledClusterMetricsService.SamplingStatus.PARTIAL));

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isPresentWith(hasBackfillStrategy(isA(DefaultSampledMetricsBackfillStrategy.class))));

        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        assertThat(metrics, hasSize(10));
        var hasPartial = hasEntry("partial", "" + true);
        for (MetricValue metric : metrics) {
            assertThat(
                metric.id(),
                either(startsWith(IX_METRIC_ID_PREFIX + ":" + indexName)).or(startsWith(RA_S_METRIC_ID_PREFIX + ":" + indexName))
            );
            assertThat(metric.type(), is(either(equalTo("es_indexed_data")).or(equalTo("es_raw_stored_data"))));
            assertThat(metric.sourceMetadata(), hasEntry("index", indexName));
            assertThat(metric.sourceMetadata(), hasPartial);

            var isFailed = metric.id().endsWith(":" + failedIndex);
            if (isFailed) {
                assertThat(metric.value(), is(0L));
            } else {
                assertThat(metric.value(), is(greaterThanOrEqualTo(10L)));
            }
        }
    }

    public void testNoPersistentTaskNode() {
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();
        indexInfoService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.NO_NODE;

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    public void testAnotherNodeIsPersistentTaskNode() {
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();
        indexInfoService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.ANOTHER_NODE;

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }

    public void testThisNodeIsPersistentTaskNodeButNotReady() {
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();
        indexInfoService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }
}
