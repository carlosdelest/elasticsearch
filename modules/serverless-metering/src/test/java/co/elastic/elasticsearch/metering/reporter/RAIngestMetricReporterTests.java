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

package co.elastic.elasticsearch.metering.reporter;

import co.elastic.elasticsearch.metering.IngestMetricsProvider;

import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RAIngestMetricReporterTests extends ESTestCase {
    IngestMetricsProvider ingestMetricsProvider = mock(IngestMetricsProvider.class);
    String indexName = "indexName";
    RAIngestMetricReporter raIngestMetricReporter = new RAIngestMetricReporter(indexName, ingestMetricsProvider);
    ParsedDocument parsedDocument = mock(ParsedDocument.class);
    XContentParserDecorator parserDecorator = mock(XContentParserDecorator.class);

    public void testZeroMeteredIsNotReported() {
        //empty instance returns 0
        when(parsedDocument.getNormalizedSize()).thenReturn(XContentMeteringParserDecorator.UNKNOWN_SIZE);
        raIngestMetricReporter.onIndexingCompleted(parsedDocument);

        verify(ingestMetricsProvider, times(0)).addIngestedDocValue(any(String.class), any(Long.class));
    }

    public void testMeteredValueIsReported() {
        when(parsedDocument.getNormalizedSize()).thenReturn(123L);
        raIngestMetricReporter.onIndexingCompleted(parsedDocument);

        verify(ingestMetricsProvider).addIngestedDocValue(eq(indexName), eq(123L));
    }

}
