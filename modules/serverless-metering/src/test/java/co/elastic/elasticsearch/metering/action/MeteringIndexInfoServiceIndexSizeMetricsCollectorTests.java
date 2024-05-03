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

package co.elastic.elasticsearch.metering.action;

import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static java.util.Map.entry;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeteringIndexInfoServiceIndexSizeMetricsCollectorTests extends ESTestCase {
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.builder().put(SEARCH_POWER_MIN_SETTING.getKey(), 100).put(SEARCH_POWER_MAX_SETTING.getKey(), 200).build(),
        Set.of(SEARCH_POWER_MIN_SETTING, SEARCH_POWER_MAX_SETTING, SEARCH_POWER_SETTING)
    );

    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    private void setInternalIndexInfoServiceData(MeteringIndexInfoService indexInfoService, Map<ShardId, MeteringShardInfo> data) {
        setInternalIndexInfoServiceData(indexInfoService, data, Set.of());
    }

    private void setInternalIndexInfoServiceData(
        MeteringIndexInfoService indexInfoService,
        Map<ShardId, MeteringShardInfo> data,
        Set<MeteringIndexInfoService.CollectedMeteringShardInfoFlag> flags
    ) {
        indexInfoService.collectedShardInfo.set(new MeteringIndexInfoService.CollectedMeteringShardInfo(data, flags));
        indexInfoService.isPersistentTaskNode = true;
    }

    public void testGetMetrics() {
        String indexName = "myIndex";
        int shardIdInt = 0;
        var shard1Id = new ShardId(indexName, "index1UUID", shardIdInt);

        var shardsInfo = Map.ofEntries(entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1)));
        var indexInfoService = new MeteringIndexInfoService();
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var indexSizeMetricsCollector = indexInfoService.createIndexSizeMetricsCollector(clusterService, Settings.EMPTY);

        Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

        assertThat(metrics, hasSize(1));

        var metric = (MetricsCollector.MetricValue) metrics.toArray()[0];
        assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
        assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shardIdInt));
        assertThat(metric.type(), equalTo("es_indexed_data"));
        assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));

        assertThat(metric.value(), is(11L));
    }

    public void testMultipleShards() {
        String indexName = "myMultiShardIndex";

        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new ShardId(indexName, "index1UUID", id);
            return entry(shardId, new MeteringShardInfo(10L + id, 110L, 1, 1));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new MeteringIndexInfoService();
        setInternalIndexInfoServiceData(indexInfoService, shardsInfo);
        var indexSizeMetricsCollector = indexInfoService.createIndexSizeMetricsCollector(clusterService, Settings.EMPTY);

        Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

        assertThat(metrics, hasSize(10));
        int shard = 0;
        for (MetricsCollector.MetricValue metric : metrics) {
            assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
            assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shard));
            assertThat(metric.type(), equalTo("es_indexed_data"));
            assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shard)));
            assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
            shard++;
        }
    }

    public void testFailedShards() {
        String indexName = "myMultiShardIndex";
        int failedIndex = 7;

        var shardsInfo = IntStream.range(0, 10).mapToObj(id -> {
            var shardId = new ShardId(indexName, "index1UUID", id);
            var size = failedIndex == id ? 0 : 10L + id;
            return entry(shardId, new MeteringShardInfo(size, 110L, 1, 1));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));

        var indexInfoService = new MeteringIndexInfoService();
        setInternalIndexInfoServiceData(
            indexInfoService,
            shardsInfo,
            Set.of(MeteringIndexInfoService.CollectedMeteringShardInfoFlag.PARTIAL)
        );

        var indexSizeMetricsCollector = indexInfoService.createIndexSizeMetricsCollector(clusterService, Settings.EMPTY);
        Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

        assertThat(metrics, hasSize(10));
        var hasPartial = hasEntry("partial", "" + true);
        int shard = 0;
        for (MetricsCollector.MetricValue metric : metrics) {
            assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
            assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shard));
            assertThat(metric.type(), equalTo("es_indexed_data"));
            assertThat(metric.metadata(), hasEntry("index", indexName));
            assertThat(metric.metadata(), hasEntry("shard", "" + shard));
            assertThat(metric.metadata(), hasPartial);
            if (shard == failedIndex) {
                assertThat(metric.value(), is(0L));
            } else {
                assertThat(metric.value(), is(both(greaterThanOrEqualTo(10L)).and(lessThanOrEqualTo(20L))));
            }
            shard++;
        }
    }

    public void testEmptyIndexInfo() {
        var indexInfoService = new MeteringIndexInfoService();

        var indexSizeMetricsCollector = indexInfoService.createIndexSizeMetricsCollector(clusterService, Settings.EMPTY);
        Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

        assertThat(metrics, hasSize(0));
    }
}
