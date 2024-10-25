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

import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskExecutor;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IngestMeteringIT extends AbstractMeteringIntegTestCase {
    private static final int ASCII_SIZE = 1;
    private static final int NUMBER_SIZE = Long.BYTES;

    static final int PIPELINE_ADDED_FIELDS_SIZE = (13 + 4) * ASCII_SIZE + 16 * ASCII_SIZE + 1;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var list = new ArrayList<>(super.nodePlugins());
        list.add(InternalSettingsPlugin.class);
        list.add(IngestCommonPlugin.class);
        list.add(DataStreamsPlugin.class);
        list.add(TestScriptPlugin.class);
        return list;
    }

    @Before
    public void init() {
        createNewFieldPipeline();
    }

    public void testIngestMetricsAreRecordedThroughBulk() {
        String indexName = "idx1";
        startMasterAndIndexNode();
        startSearchNode();
        createIndex(indexName);
        // size 3*char+int (long size)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName);
        assertUsageRecord(indexName, usageRecord, 3 * ASCII_SIZE + NUMBER_SIZE);

        receivedMetrics().clear();

        // size 3*char+int (long size)
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();

        usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName);
        assertUsageRecord(indexName, usageRecord, 3 * ASCII_SIZE + NUMBER_SIZE);
        assertThat(usageRecord, transformedMatch(metric -> metric.source().metadata().get("datastream"), nullValue()));
    }

    public void testIngestMetricsAreRecordedForDataStreams() throws Exception {
        String indexName = "idx1";
        String dsName = ".ds-" + indexName;

        startMasterAndIndexNode();
        startSearchNode();

        createDataStream(indexName);

        // size 16*char+int (long size)
        client().index(
            new IndexRequest(indexName).source(XContentType.JSON, "@timestamp", 123, "key", "abc").opType(DocWriteRequest.OpType.CREATE)
        ).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(dsName);
        assertUsageRecord(dsName, usageRecord, 16 * ASCII_SIZE + NUMBER_SIZE);
        assertThat(
            usageRecord,
            transformedMatch((UsageRecord metric) -> metric.source().metadata().get("datastream"), startsWith(indexName))
        );
    }

    public void testIngestMetricsAreRecordedAfterIngestPipelines() {
        String indexName2 = "idx2";
        startMasterIndexAndIngestNode();
        startSearchNode();
        createIndex(indexName2);

        client().index(
            // size 3*char+int (long size)
            new IndexRequest(indexName2).setPipeline("new_field_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")
        ).actionGet();

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName2);
        assertUsageRecord(indexName2, usageRecord, 3 * ASCII_SIZE + NUMBER_SIZE + PIPELINE_ADDED_FIELDS_SIZE);

        receivedMetrics().clear();

        client().index(
            // size 3*char+int (long size)
            new IndexRequest(indexName2).setPipeline("new_field_pipeline").id("1").source(XContentType.JSON, "a", 1, "b", "c")
        ).actionGet();

        usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName2);
        assertUsageRecord(indexName2, usageRecord, 3 * ASCII_SIZE + NUMBER_SIZE + PIPELINE_ADDED_FIELDS_SIZE);
        assertThat(usageRecord, transformedMatch(metric -> metric.source().metadata().get("datastream"), nullValue()));
    }

    public void testDocumentFailingInPipelineNotReported() {
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

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName3);
        // even though 2 documents were in bulk request, we will only have 1 reported
        assertUsageRecord(indexName3, usageRecord, 3 * ASCII_SIZE + NUMBER_SIZE + PIPELINE_ADDED_FIELDS_SIZE);
    }

    public void testUpdatesByDocumentAreMetered() throws IOException {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName4 = "update_partial_doc";
        createIndex(indexName4);
        indexDoc(indexName4, 3 * ASCII_SIZE + NUMBER_SIZE, "a", 1, "b", "c");

        client().prepareUpdate().setIndex(indexName4).setId("1").setDoc(jsonBuilder().startObject().field("d", 2).endObject()).get();

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName4);
        assertUsageRecord(indexName4, usageRecord, (3 * ASCII_SIZE + NUMBER_SIZE) + (ASCII_SIZE + NUMBER_SIZE));// updated size
    }

    public void testUpdatesViaScriptAreNotMetered() {
        startMasterIndexAndIngestNode();
        startSearchNode();
        String indexName = "index1";
        createIndex(indexName);

        String scriptId = "script1";
        putJsonStoredScript(scriptId, Strings.format("""
            {"script": {"lang": "%s", "source": "ctx._source.b = 'xx'"} }""", MockScriptEngine.NAME));

        indexDoc(indexName, 3 * ASCII_SIZE + NUMBER_SIZE, "a", 1, "b", "c");

        // update via stored script
        final Script storedScript = new Script(ScriptType.STORED, null, scriptId, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(storedScript).get();

        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName);
        assertUsageRecord(indexName, usageRecord, (3 * ASCII_SIZE + NUMBER_SIZE) + (1 * ASCII_SIZE)); // updated size
        receivedMetrics().clear();

        // update via inlined script
        String scriptCode = "ctx._source.b = 'xx'";
        final Script script = new Script(ScriptType.INLINE, TestScriptPlugin.NAME, scriptCode, Collections.emptyMap());
        client().prepareUpdate().setIndex(indexName).setId("1").setScript(script).get();

        usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName);
        assertUsageRecord(indexName, usageRecord, (3 * ASCII_SIZE + NUMBER_SIZE) + (1 * ASCII_SIZE)); // updated size
        receivedMetrics().clear();
    }

    private void indexDoc(String indexName, int expectedIngestSize, Object... source) {
        client().index(new IndexRequest(indexName).id("1").source(XContentType.JSON, source)).actionGet();
        client().admin().indices().prepareFlush(indexName).get().getStatus().getStatus();
        UsageRecord usageRecord = pollReceivedRAIRecordsAndGetFirst(indexName);

        assertUsageRecord(indexName, usageRecord, expectedIngestSize);
        receivedMetrics().clear();
    }

    private static void assertUsageRecord(String indexName, UsageRecord metric, int expectedQuantity) {
        String id = "ingested-doc:" + indexName;
        assertThat(metric.id(), startsWith(id));
        assertThat(metric.usage().type(), equalTo("es_raw_data"));
        assertThat(metric.usage().quantity(), equalTo((long) expectedQuantity));
        assertThat(metric.source().metadata(), hasEntry(equalTo("index"), startsWith(indexName)));
    }

    private void createFailPipeline() {
        putJsonPipeline("fail_pipeline", """
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
    }

    static void createNewFieldPipeline() {
        putJsonPipeline("new_field_pipeline", """
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
    }

    private UsageRecord pollReceivedRAIRecordsAndGetFirst(String indexName) {
        waitUntil(() -> hasReceivedRecords("ingested-doc:" + indexName));
        List<UsageRecord> usageRecords = new ArrayList<>();
        pollReceivedRecords(usageRecords);
        return usageRecords.stream().filter(m -> m.id().startsWith("ingested-doc:" + indexName)).findFirst().get();
    }
}
