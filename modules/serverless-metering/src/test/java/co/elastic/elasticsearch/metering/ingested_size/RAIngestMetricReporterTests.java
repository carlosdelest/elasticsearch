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

package co.elastic.elasticsearch.metering.ingested_size;

import co.elastic.elasticsearch.metering.IngestMetricsCollector;

import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RAIngestMetricReporterTests extends ESTestCase {
    IngestMetricsCollector ingestMetricsCollector = mock(IngestMetricsCollector.class);
    String indexName = "indexName";
    RAIngestMetricReporter raIngestMetricReporter = new RAIngestMetricReporter(indexName, ingestMetricsCollector);
    ParsedDocument parsedDocument = mock(ParsedDocument.class);
    DocumentSizeObserver documentSizeObserver = mock(DocumentSizeObserver.class);

    public void testZeroMeteredIsNotReported() {
        //empty instance returns 0
        when(parsedDocument.getDocumentSizeObserver()).thenReturn(DocumentSizeObserver.EMPTY_INSTANCE);
        raIngestMetricReporter.onIndexingCompleted(parsedDocument);

        verify(ingestMetricsCollector, times(0)).addIngestedDocValue(any(String.class), any(Long.class));
    }

    public void testMeteredValueIsReported() {
        when(parsedDocument.getDocumentSizeObserver()).thenReturn(documentSizeObserver);
        when(documentSizeObserver.normalisedBytesParsed()).thenReturn(123L);
        raIngestMetricReporter.onIndexingCompleted(parsedDocument);

        verify(ingestMetricsCollector).addIngestedDocValue(eq(indexName), eq(123L));
    }

}
