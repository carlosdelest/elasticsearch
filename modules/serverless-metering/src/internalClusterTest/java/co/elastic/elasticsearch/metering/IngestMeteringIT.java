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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IngestMeteringIT extends AbstractMeteringIntegTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);
    protected static final int DEFAULT_SEARCH_POWER = 200;

    Map<String, Object> defaultAttributes = Map.of(
        "boost_window",
        (int) DEFAULT_BOOST_WINDOW.seconds(),
        "search_power",
        DEFAULT_SEARCH_POWER
    );

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
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var list = new ArrayList<Class<? extends Plugin>>();
        list.addAll(super.nodePlugins());
        list.add(InternalSettingsPlugin.class);
        list.add(IngestCommonPlugin.class);
        list.add(CustomScriptPlugin.class);
        return list;
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
        assertUsageRecord(indexName, usageRecord, expectedDefaultAttributes, 3 * Character.SIZE + Long.SIZE);

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
        assertUsageRecord(indexName, usageRecord, expectedOverriddenAttributes, 3 * Character.SIZE + Long.SIZE);

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
        assertUsageRecord(indexName2, usageRecord, expectedDefaultAttributes, 3 * Character.SIZE + Long.SIZE);

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
        assertUsageRecord(indexName2, usageRecord, expectedOverriddenAttributes, 3 * Character.SIZE + Long.SIZE);
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
        assertUsageRecord(indexName3, usageRecord, expectedDefaultAttributes, 3 * Character.SIZE + Long.SIZE);
    }

    public void testUpdatesAreMeteredInBulkRawWithPartialDoc() throws InterruptedException, IOException {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String partialDocUpdate = "update_partial_doc";
        createIndex(partialDocUpdate);
        indexDoc(partialDocUpdate, defaultAttributes);

        client().prepareUpdate().setIndex(partialDocUpdate).setId("1").setDoc(jsonBuilder().startObject().field("d", 2).endObject()).get();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + partialDocUpdate);
        assertUsageRecord(partialDocUpdate, usageRecord, expectedDefaultAttributes, Character.SIZE + Long.SIZE);// partial doc size
    }

    public void testUpdatesViaScriptAreNotMetered() throws InterruptedException, IOException {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName = "index1";
        createIndex(indexName);

        String scriptId = "script1";
        clusterAdmin().preparePutStoredScript().setId(scriptId).setContent(new BytesArray(Strings.format("""
            {"script": {"lang": "%s", "source": "ctx._source.b = 'xx'"} }""", MockScriptEngine.NAME)), XContentType.JSON).get();

        // combining an index and 2 updates and expecting only the metering value for the new indexed doc & partial update
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, "a", 1, "b", "c")).actionGet();

        // update via stored script
        final Script storedScript = new Script(ScriptType.STORED, null, scriptId, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(storedScript).get();

        // update via inlined script
        String scriptCode = "ctx._source.b = 'xx'";
        final Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, scriptCode, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(script).get();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName);

        assertUsageRecord(indexName, usageRecord, defaultAttributes, 3 * Character.SIZE + Long.SIZE);
        receivedMetrics().clear();
    }

    private void indexDoc(String indexName, Map<String, Object> settings) throws InterruptedException {
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, "a", 1, "b", "c")).actionGet();
        client().admin().indices().prepareFlush(indexName).get().getStatus().getStatus();
        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord usageRecord = getFirstReceivedRecord("ingested-doc:" + indexName);

        assertUsageRecord(indexName, usageRecord, settings, 3 * Character.SIZE + Long.SIZE);
        receivedMetrics().clear();
    }

    private static void assertUsageRecord(String indexName, UsageRecord metric, Map<String, Object> settings, int expectedQuantity) {
        String id = "ingested-doc:" + indexName;
        assertThat(metric.id(), startsWith(id));
        assertThat(metric.usage().type(), equalTo("es_raw_data"));
        assertThat(metric.usage().quantity(), equalTo((long) expectedQuantity));
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

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("ctx._source.b = 'xx'", vars -> srcScript(vars, source -> { return source.replace("b", "xx"); }));
            return scripts;
        }

        @SuppressWarnings("unchecked")
        static Object srcScript(Map<String, Object> vars, Function<Map<String, Object>, Object> f) {
            Map<?, ?> ctx = (Map<?, ?>) vars.get("ctx");

            Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
            return f.apply(source);
        }

    }

}
