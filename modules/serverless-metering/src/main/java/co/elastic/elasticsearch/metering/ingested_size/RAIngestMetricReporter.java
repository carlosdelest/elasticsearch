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
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

public class RAIngestMetricReporter implements DocumentSizeReporter {
    private final IngestMetricsCollector ingestMetricsCollector;
    private final String indexName;

    public RAIngestMetricReporter(String indexName, IngestMetricsCollector ingestMetricsCollector) {
        this.indexName = indexName;
        this.ingestMetricsCollector = ingestMetricsCollector;
    }

    @Override
    public void onParsingCompleted(ParsedDocument parsedDocument) {
        // noop
    }

    @Override
    public void onIndexingCompleted(ParsedDocument parsedDocument) {
        DocumentSizeObserver documentSizeObserver = parsedDocument.getDocumentSizeObserver();
        var normalizedBytesParsed = documentSizeObserver.normalisedBytesParsed();
        if (documentSizeObserver.isUpdateByScript() == false && normalizedBytesParsed > 0) {
            this.ingestMetricsCollector.addIngestedDocValue(indexName, normalizedBytesParsed);
        }
    }

}
