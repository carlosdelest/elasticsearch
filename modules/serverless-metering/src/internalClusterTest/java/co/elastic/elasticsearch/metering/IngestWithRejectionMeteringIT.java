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

import co.elastic.elasticsearch.metering.xcontent.MeteringDocumentParsingProvider;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IngestWithRejectionMeteringIT extends AbstractMeteringIntegTestCase {

    private static final int ITEMS_IN_BULK = 100;
    private long documentNormalizedSize = meterDocument();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var list = new ArrayList<Class<? extends Plugin>>();
        list.addAll(super.nodePlugins());
        list.add(InternalSettingsPlugin.class);
        list.add(IngestCommonPlugin.class);
        return list;
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 1;
    }

    public void testDocumentsNotMeteredWhenRejectionInBulk() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        ensureStableCluster(2);

        String index = "test1";
        assertAcked(prepareCreate(index));
        ensureGreen();
        BulkRequest request1 = createBulkRequest(index, false);
        BulkRequest request2 = createBulkRequest(index, false);

        // expect 429 response on one request due to one write thread with write queue of size 1
        long successCount = executeRequests(request1, request2);
        ensureGreen();
        if (successCount > 0) {
            assertBusy(() -> {
                long reportedQuantity = receivedMetrics().stream()
                    .flatMap(List::stream)
                    .filter(m -> m.id().startsWith("ingested-doc"))
                    .mapToLong(r -> r.usage().quantity())
                    .sum();
                assertThat(reportedQuantity, equalTo(successCount * documentNormalizedSize));
            });
        }
    }

    public void testDocumentsNotMeteredWhenRejectionAfterPipeline() throws Exception {
        startMasterIndexAndIngestNode();
        startSearchNode();
        ensureStableCluster(2);
        IngestMeteringIT.createNewFieldPipeline();

        String index = "test2";
        assertAcked(prepareCreate(index));
        ensureGreen();

        BulkRequest request1 = createBulkRequest(index, true);
        BulkRequest request2 = createBulkRequest(index, true);

        // expect 429 response on one request due to one write thread with write queue of size 1
        long successCount = executeRequests(request1, request2);
        ensureGreen();
        if (successCount > 0) {
            assertBusy(() -> {
                long reportedQuantity = receivedMetrics().stream()
                    .flatMap(List::stream)
                    .filter(m -> m.id().startsWith("ingested-doc"))
                    .mapToLong(r -> r.usage().quantity())
                    .sum();
                assertThat(
                    reportedQuantity,
                    equalTo(successCount * (documentNormalizedSize + IngestMeteringIT.PIPELINE_ADDED_FIELDS_SIZE))
                );
            });

        }
    }

    private static BulkRequest createBulkRequest(String index, boolean withPipeline) {
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < ITEMS_IN_BULK; ++i) {
            IndexRequest indexRequest = new IndexRequest(index).source(documentSource());
            if (withPipeline) {
                indexRequest.setPipeline("pipeline");
            }
            request.add(indexRequest).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        return request;
    }

    private long executeRequests(BulkRequest request1, BulkRequest request2) throws IOException {
        long successCount = 0;
        final ActionFuture<BulkResponse> bulkFuture1 = client().bulk(request1);
        final ActionFuture<BulkResponse> bulkFuture2 = client().bulk(request2);
        try {
            BulkResponse bulkItemResponses1 = bulkFuture1.actionGet();
            logger.info("response 1: " + formatResponse(bulkItemResponses1));
            successCount += Arrays.stream(bulkItemResponses1.getItems()).filter(b -> b.isFailed() == false).count();
        } catch (EsRejectedExecutionException e) {
            logger.info(e);
            // ignored, one of the two bulk requests was rejected outright due to the write queue being full
        }
        try {
            BulkResponse bulkItemResponses2 = bulkFuture2.actionGet();
            logger.info("response 2: " + formatResponse(bulkItemResponses2));
            successCount += Arrays.stream(bulkItemResponses2.getItems()).filter(b -> b.isFailed() == false).count();
        } catch (EsRejectedExecutionException e) {
            logger.info(e);
            // ignored, one of the two bulk requests was rejected outright due to the write queue being full
        }
        logger.info("success count " + successCount);
        return successCount;
    }

    private String formatResponse(BulkResponse bulkItemResponses) {
        return bulkItemResponses.hasFailures() ? bulkItemResponses.buildFailureMessage() : "success";
    }

    private static Map<String, String> documentSource() {
        return Collections.singletonMap("key", "value");
    }

    private long meterDocument() {
        try {
            BytesReference bytesReference = XContentTestUtils.convertToXContent(documentSource(), XContentType.JSON);
            MeteringDocumentParsingProvider provider = new MeteringDocumentParsingProvider(false, Mockito::mock, Mockito::mock);
            XContentMeteringParserDecorator meteringParserDecorator = provider.newMeteringParserDecorator(new IndexRequest());

            XContentHelper.convertToMap(bytesReference, false, XContentType.JSON, meteringParserDecorator).v2();

            assertThat(meteringParserDecorator.meteredDocumentSize(), greaterThan(0L));
            return meteringParserDecorator.meteredDocumentSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
