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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IngestMeteringIT extends AbstractMeteringIntegTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    protected static final int DEFAULT_SEARCH_POWER = 200;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), IngestCommonPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), DEFAULT_BOOST_WINDOW)
            .put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), DEFAULT_SEARCH_POWER)
            .build();
    }

    protected String startNodes() {
        return internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE)
        );
    }

    public void testIngestMetricsAreRecordedThroughIndexing() throws InterruptedException, IOException {
        startNodes();
        Map<String, Object> settings = Map.of("boost_window", (int) DEFAULT_BOOST_WINDOW.seconds(), "search_power", DEFAULT_SEARCH_POWER);
        // a usage record is reported when using raw bulk apis
        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();// size 3*char+int (long size)

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));

        UsageRecord usageRecord = getUsageRecords("ingested-doc:" + indexName);

        assertUsageRecord(indexName, usageRecord, settings);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder().put("serverless.search.boost_window", TimeValue.timeValueDays(3)).put("serverless.search.search_power", 1234)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        receivedMetrics().clear();

        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();// size 3*char+int (long size)

        // the same value is reported when using ingest pipelines
        String indexName2 = "idx2";
        createPipeline("pipeline");
        client().index(
            new IndexRequest(indexName2).setPipeline("pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")// size 3*char+int (long
                                                                                                                    // size)
        ).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));

        usageRecord = getUsageRecords("ingested-doc:" + indexName2);
        assertUsageRecord(
            indexName2,
            usageRecord,
            Map.of("boost_window", (int) TimeValue.timeValueDays(3).seconds(), "search_power", 1234)
        );
    }

    private static void assertUsageRecord(String indexName, UsageRecord metric, Map<String, Object> settings) {
        String id = "ingested-doc:" + indexName;
        assertThat(metric.id(), startsWith(id));
        assertThat(metric.usage().type(), equalTo("es_raw_data"));
        assertThat(metric.usage().quantity(), equalTo(((long) (3 * Character.SIZE + Long.SIZE))));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName)));
        settings.forEach((k, v) -> assertThat(metric.usage().es().get(k), equalTo(v)));
    }

    private void createPipeline(String pipeline) {
        final BytesReference pipelineBody = new BytesArray("""
            {
              "processors": [
                {
                   "set": {
                     "field": "my-text-field",
                     "value": "xxxx"
                   }
                 },
                 {
                   "set": {
                     "field": "my-boolean-field",
                     "value": true
                   }
                 }
              ]
            }
            """);
        clusterAdmin().putPipeline(new PutPipelineRequest(pipeline, pipelineBody, XContentType.JSON)).actionGet();
    }
}
