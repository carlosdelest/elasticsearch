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
    protected static final int DEFAULT_SEARCH_POWER_SELECTED = 100;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), DEFAULT_BOOST_WINDOW)
            .build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 1;
    }

    public void testNodeCanStartWithMeteringEnabled() {
        startMasterAndIndexNode();
        startSearchNode();
        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(MeteringPlugin.class))
            .toList();
        assertThat(plugins, not(empty()));
    }

    public void testSizeMetricsAreRecorded() throws InterruptedException {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> hasReceivedRecords("shard-size"));
        UsageRecord metric = pollReceivedRecordsAndGetFirst("shard-size");

        String idPRefix = "shard-size:" + indexName + ":0";
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        assertThat(metric.usage().es().get("search_power"), equalTo(DEFAULT_SEARCH_POWER_SELECTED));
        assertThat(metric.usage().es().get("boost_window"), equalTo(null));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName, "shard", "0")));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
                .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 1500)
                .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 1234)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        waitUntil(() -> hasReceivedRecords("shard-size"));
        metric = pollReceivedRecordsAndGetFirst("shard-size");
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        assertThat(metric.usage().es().get("search_power"), equalTo(1234));
        assertThat(metric.usage().es().get("boost_window"), equalTo(null));

        ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
                .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 30)
        );
        assertAcked(client().admin().cluster().updateSettings(clusterUpdateSettingsRequest).actionGet());

        waitUntil(() -> hasReceivedRecords("shard-size"));
        metric = pollReceivedRecordsAndGetFirst("shard-size");
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        // for now if we update search_power_range we set search_power to be equal to search_power_min
        assertThat(metric.usage().es().get("search_power"), equalTo(30));
        assertThat(metric.usage().es().get("boost_window"), equalTo(null));
    }

    public void testSearchPowerUpdates() throws InterruptedException {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> hasReceivedRecords("shard-size"));
        UsageRecord metric = pollReceivedRecordsAndGetFirst("shard-size");
        assertThat(metric.usage().es().get("search_power"), equalTo(DEFAULT_SEARCH_POWER_SELECTED));

        {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
                    .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 1000)
                    .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 1500)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(1000));
        }
        {
            ClusterUpdateSettingsRequest updateMinTooHigh = new ClusterUpdateSettingsRequest();
            updateMinTooHigh.persistentSettings(Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 2000));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().cluster().updateSettings(updateMinTooHigh).actionGet()
            );
            assertEquals(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                    + " [2000] must be <= "
                    + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                    + " [1500]",
                e.getMessage()
            );
        }
        {
            ClusterUpdateSettingsRequest updateMaxTooLow = new ClusterUpdateSettingsRequest();
            updateMaxTooLow.persistentSettings(Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 100));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().cluster().updateSettings(updateMaxTooLow).actionGet()
            );
            assertEquals(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                    + " [1000] must be <= "
                    + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                    + " [100]",
                e.getMessage()
            );
        }
        {
            ClusterUpdateSettingsRequest updateSPMinEqualSPMax = new ClusterUpdateSettingsRequest();
            updateSPMinEqualSPMax.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
                    .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 1200)
                    .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 1200)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSPMinEqualSPMax).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(1200));
        }
        {
            ClusterUpdateSettingsRequest updateSPWhenSPMinEqualSPMax = new ClusterUpdateSettingsRequest();
            updateSPWhenSPMinEqualSPMax.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
                    .put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), 1300)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSPWhenSPMinEqualSPMax).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(1300));
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSPWhenSPMinEqualSPMax.persistentSettings()).intValue(),
                equalTo(1300)
            );
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSPWhenSPMinEqualSPMax.persistentSettings()).intValue(),
                equalTo(1300)
            );
        }
    }

    public void testSearchPowerUpdatesDefaultThenRangeThenSPThenRange() throws InterruptedException {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> hasReceivedRecords("shard-size"));
        UsageRecord metric = pollReceivedRecordsAndGetFirst("shard-size");
        assertThat(metric.usage().es().get("search_power"), equalTo(DEFAULT_SEARCH_POWER_SELECTED));

        {
            ClusterUpdateSettingsRequest updateSettingsRangeRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRangeRequest.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 500)
                    .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 500)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRangeRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(500));
        }
        {
            ClusterUpdateSettingsRequest updateSettingsSPRequest = new ClusterUpdateSettingsRequest();
            updateSettingsSPRequest.persistentSettings(
                Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), 1000)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsSPRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(1000));
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(1000)
            );
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(1000)
            );
        }
        {
            ClusterUpdateSettingsRequest updateSettingsRangeRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRangeRequest.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 700)
                    .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 700)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRangeRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(700));
        }
    }

    public void testSearchPowerUpdatesDefaultThenSPThenRangeThenSP() throws InterruptedException {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> hasReceivedRecords("shard-size"));
        UsageRecord metric = pollReceivedRecordsAndGetFirst("shard-size");
        assertThat(metric.usage().es().get("search_power"), equalTo(DEFAULT_SEARCH_POWER_SELECTED));

        {
            ClusterUpdateSettingsRequest updateSettingsSPRequest = new ClusterUpdateSettingsRequest();
            updateSettingsSPRequest.persistentSettings(
                Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), 1000)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsSPRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(1000));
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(1000)
            );
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(1000)
            );
        }
        {
            ClusterUpdateSettingsRequest updateSettingsRangeRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRangeRequest.persistentSettings(
                Settings.builder()
                    .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 500)
                    .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), 500)
            );
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRangeRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(500));
        }
        {
            ClusterUpdateSettingsRequest updateSettingsSPRequest = new ClusterUpdateSettingsRequest();
            updateSettingsSPRequest.persistentSettings(Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), 800));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsSPRequest).actionGet());
            waitUntil(() -> hasReceivedRecords("shard-size"));
            metric = pollReceivedRecordsAndGetFirst("shard-size");
            assertThat(metric.usage().es().get("search_power"), equalTo(800));
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(800)
            );
            assertThat(
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updateSettingsSPRequest.persistentSettings()).intValue(),
                equalTo(800)
            );
        }
    }
}
