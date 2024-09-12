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

import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
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

import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledStorageMetricsProvider.IX_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledStorageMetricsProvider.RA_S_METRIC_ID_PREFIX;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static java.util.Map.entry;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledStorageMetricsProviderTests extends ESTestCase {
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.builder().put(SEARCH_POWER_MIN_SETTING.getKey(), 100).put(SEARCH_POWER_MAX_SETTING.getKey(), 200).build(),
        Set.of(SEARCH_POWER_MIN_SETTING, SEARCH_POWER_MAX_SETTING)
    );

    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
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
        indexInfoService.collectedShardInfo.set(new SampledClusterMetricsService.SampledClusterMetrics(data, flags));
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
                    new ShardInfoMetrics(11L, 110L, 1, 1, 11L, Instant.now().toEpochMilli())
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
        assertThat(metric1.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));
        assertThat(metric1.value(), is(11L));
        assertThat(metric1.meteredObjectCreationTime(), notNullValue());
        assertTrue(metric1.meteredObjectCreationTime().isAfter(Instant.EPOCH));

        assertThat(metric2.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(metric2.metadata(), equalTo(Map.of("index", indexName)));
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
                    new ShardInfoMetrics(11L, 110L, 1, 1, 0, creationDate.toEpochMilli())
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        assertThat(metrics, hasSize(1));

        var metric = (MetricValue) metrics.toArray()[0];
        assertThat(metric.id(), equalTo(IX_METRIC_ID_PREFIX + ":" + indexName + ":" + shardIdInt));
        assertThat(metric.type(), equalTo("es_indexed_data"));
        assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));

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
                    new ShardInfoMetrics(0, 110L, 1, 1, 11L, creationDate.toEpochMilli())
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        assertThat(metrics, hasSize(1));

        var metric = (MetricValue) metrics.toArray()[0];
        assertThat(metric.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
        assertThat(metric.type(), equalTo("es_raw_stored_data"));
        assertThat(metric.metadata(), equalTo(Map.of("index", indexName)));

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
                    new ShardInfoMetrics(0, 110L, 1, 1, 0, creationDate.toEpochMilli())
                )
            )
        );
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
                    new ShardInfoMetrics(size, 110L, 1, 1, hasIngestSize ? size : 0L, creationDate.toEpochMilli())
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
            assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shard)));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
            shard++;
        }

        for (MetricValue metric : rawStoredDataMetrics) {
            assertThat(metric.id(), equalTo(RA_S_METRIC_ID_PREFIX + ":" + indexName));
            assertThat(metric.metadata(), equalTo(Map.of("index", indexName)));
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
                        new ShardInfoMetrics(size, 110L, 1, 1, hasIngestSize ? size : 0L, creationDate.toEpochMilli())
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
            assertThat(metric.metadata(), aMapWithSize(2));
            assertThat(metric.metadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.metadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        assertThat(
            rawStoredDataMetrics.stream().map(MetricValue::id).toList(),
            containsInAnyOrder(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 0, RA_S_METRIC_ID_PREFIX + ":" + baseIndexName + 1)
        );
        var metadataList = rawStoredDataMetrics.stream().map(MetricValue::metadata).toList();
        assertThat(metadataList, everyItem(aMapWithSize(1)));
        assertThat(metadataList, everyItem(hasEntry(is("index"), startsWith(baseIndexName))));
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
                        new ShardInfoMetrics(size, 110L, 1, 1, hasIngestSize ? size : 0, creationDate.toEpochMilli())
                    )
                );
            }
        }

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
            assertThat(metric.metadata(), aMapWithSize(2));
            assertThat(metric.metadata(), hasEntry(is("index"), startsWith(baseIndexName)));
            assertThat(metric.metadata(), hasEntry(is("shard"), not(emptyOrNullString())));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
        }

        for (MetricValue metric : rawStoredDataMetrics) {
            assertThat(metric.id(), startsWith(RA_S_METRIC_ID_PREFIX + ":" + baseIndexName));
            assertThat(metric.metadata(), aMapWithSize(1));
            assertThat(metric.metadata(), hasEntry(is("index"), startsWith(baseIndexName)));
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
                    new ShardInfoMetrics(size, 110L, 1, 1, size, creationDate.toEpochMilli())
                )
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo, Set.of(SampledClusterMetricsService.SamplingStatus.PARTIAL));

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();

        var metricValues = sampledStorageMetricsProvider.getMetrics();
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
            assertThat(metric.metadata(), hasEntry("index", indexName));
            assertThat(metric.metadata(), hasPartial);

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
        Collection<MetricValue> metrics = iterableToList(
            metricValues.orElseThrow(SampledStorageMetricsProviderTests::elementMustBePresent)
        );

        assertThat(metrics, hasSize(0));
    }

    public void testThisNodeIsPersistentTaskNodeButNotReady() {
        var indexInfoService = new SampledClusterMetricsService(clusterService, MeterRegistry.NOOP);

        var sampledStorageMetricsProvider = indexInfoService.createSampledStorageMetricsProvider();
        indexInfoService.persistentTaskNodeStatus = SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;

        var metricValues = sampledStorageMetricsProvider.getMetrics();
        assertThat(metricValues, isEmpty());
    }
}
