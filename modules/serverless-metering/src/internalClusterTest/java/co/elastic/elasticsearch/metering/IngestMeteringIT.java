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

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IngestMeteringIT extends AbstractMeteringIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), IngestCommonPlugin.class);
    }

    protected String startNodes() {
        return internalCluster().startNode(
            settingsForRoles(
                DiscoveryNodeRole.MASTER_ROLE,
                DiscoveryNodeRole.INDEX_ROLE,
                DiscoveryNodeRole.SEARCH_ROLE,
                DiscoveryNodeRole.INGEST_ROLE
            )
        );
    }

    public void testIngestMetricsAreRecordedThroughIndexing() throws InterruptedException, IOException {
        startNodes();
        // a usage record is reported when using raw bulk apis
        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "a", 1, "b", "c")).actionGet();// size 3*char+int (long size)

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));

        Optional<UsageRecord> maybeRecord = getUsageRecords("ingested-doc:" + indexName);

        assertUsageRecord(indexName, maybeRecord);

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

        maybeRecord = getUsageRecords("ingested-doc:" + indexName2);
        assertUsageRecord(indexName2, maybeRecord);
    }

    private static void assertUsageRecord(String indexName, Optional<UsageRecord> maybeRecord) {
        UsageRecord metric = maybeRecord.get();
        String id = "ingested-doc:" + indexName;
        assertThat(metric.id(), startsWith(id));
        assertThat(metric.usage().type(), equalTo("es_raw_data"));
        assertThat(metric.usage().quantity(), equalTo(((long) (3 * Character.SIZE + Long.SIZE))));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName)));
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
