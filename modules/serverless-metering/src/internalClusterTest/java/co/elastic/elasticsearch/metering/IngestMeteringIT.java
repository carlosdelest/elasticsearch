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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

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
    Map<String, Object> expectedDefaultAttributes = Map.of(
        "boost_window",
        (int) DEFAULT_BOOST_WINDOW.seconds(),
        "search_power",
        DEFAULT_SEARCH_POWER
    );
    Map<String, Object> expectedOverriddenAttributes = Map.of(
        "boost_window",
        (int) TimeValue.timeValueDays(3).seconds(),
        "search_power",
        1234
    );
    Settings.Builder overrideSettings = Settings.builder()
        .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(3))
        .put(ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey(), 1234);

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

    @Before
    public void init() {
        createNewFieldPipeline();
    }

    @After
    public void cleanup() {
        receivedMetrics().clear();
        assertAcked(
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testIngestMetricsAreRecordedThroughBulk() throws InterruptedException, IOException {
        String indexName = "idx1";
        startMasterAndIndexNode();
        startSearchNode();
        createIndex(indexName);
        // size 3*char+int (long size)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));

        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, expectedDefaultAttributes);

        // change settings propagated to usage records
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(overrideSettings);
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        receivedMetrics().clear();

        // size 3*char+int (long size)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName);
        assertUsageRecord(indexName, usageRecord, expectedOverriddenAttributes);
    }

    public void testIngestMetricsAreRecordedThroughIngestPipelines() throws InterruptedException, IOException {
        String indexName2 = "idx2";
        startMasterIndexAndIngestNode();
        startSearchNode();
        createIndex(indexName2);

        client().index(
            // size 3*char+int (long size), pipeline added fields not included
            new IndexRequest(indexName2).setPipeline("new_field_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")
        ).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName2);
        assertUsageRecord(indexName2, usageRecord, expectedDefaultAttributes);

        // change settings propagated to usage records
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(overrideSettings);
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        receivedMetrics().clear();

        client().index(
            // size 3*char+int (long size), pipeline added fields not included
            new IndexRequest(indexName2).setPipeline("new_field_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")
        ).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName2);
        assertUsageRecord(indexName2, usageRecord, expectedOverriddenAttributes);
    }

    public void testDocumentFailingInPipelineNotReported() throws InterruptedException, IOException {
        String indexName3 = "idx3";
        startMasterIndexAndIngestNode();
        startSearchNode();
        createIndex(indexName3);
        createFailPipeline();

        client().bulk(
            new BulkRequest().add(
                new IndexRequest(indexName3).setPipeline("new_field_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")
            )
                // size 3*char+int (long size), but it uses a fail pipeline
                .add(new IndexRequest(indexName3).setPipeline("fail_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c"))
        ).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName3);
        // even though 2 documents were in bulk request, we will only have 1 reported
        assertUsageRecord(indexName3, usageRecord, expectedDefaultAttributes);
    }

    private static void assertUsageRecord(String indexName, UsageRecord metric, Map<String, Object> settings) {
        String id = "ingested-doc:" + indexName;
        assertThat(metric.id(), startsWith(id));
        assertThat(metric.usage().type(), equalTo("es_raw_data"));
        assertThat(metric.usage().quantity(), equalTo(((long) (3 * Character.SIZE + Long.SIZE))));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName)));
        settings.forEach((k, v) -> assertThat(metric.usage().es().get(k), equalTo(v)));
    }

    private void createFailPipeline() {
        final BytesReference pipelineBody = new BytesArray("""
            {
              "processors": [
                {
                   "fail": {
                     "message": "always fail"
                   }
                 }
              ]
            }
            """);
        clusterAdmin().putPipeline(new PutPipelineRequest("fail_pipeline", pipelineBody, XContentType.JSON)).actionGet();
    }

    private void createNewFieldPipeline() {
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
        clusterAdmin().putPipeline(new PutPipelineRequest("new_field_pipeline", pipelineBody, XContentType.JSON)).actionGet();
    }
}
