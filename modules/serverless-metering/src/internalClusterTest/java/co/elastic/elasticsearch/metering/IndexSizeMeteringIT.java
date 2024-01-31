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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IndexSizeMeteringIT extends AbstractMeteringIntegTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    protected static final int DEFAULT_SEARCH_POWER = 200;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), DEFAULT_BOOST_WINDOW)
            .put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), DEFAULT_SEARCH_POWER)
            .build();
    }

    public void testNodeCanStartWithMeteringEnabled() {
        startNodes();

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(MeteringPlugin.class))
            .toList();
        assertThat(plugins, not(empty()));
    }

    public void testSizeMetricsAreRecorded() throws InterruptedException {
        startNodes();

        String indexName = "idx1";
        assertAcked(
            prepareCreate(
                indexName,
                1,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> receivedMetrics().stream().anyMatch(l -> l.stream().anyMatch(u -> u.id().startsWith("shard-size"))));
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord metric = getUsageRecords("shard-size");

        String idPRefix = "shard-size:" + indexName + ":0";
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        assertThat(metric.usage().es().get("search_power"), equalTo(DEFAULT_SEARCH_POWER));
        assertThat(metric.usage().es().get("boost_window"), equalTo(null));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName, "shard", "0")));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder().put("serverless.search.boost_window", TimeValue.timeValueDays(3)).put("serverless.search.search_power", 1234)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        metric = getUsageRecords("shard-size");
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        assertThat(metric.usage().es().get("search_power"), equalTo(1234));
        assertThat(metric.usage().es().get("boost_window"), equalTo(null));
    }
}
