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

import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

import java.util.function.Supplier;

public class MeteringDocumentParsingProvider implements DocumentParsingProvider {
    private final Supplier<IngestMetricsCollector> ingestMetricsCollectorSupplier;

    public MeteringDocumentParsingProvider(Supplier<IngestMetricsCollector> ingestMetricsCollectorSupplier) {
        this.ingestMetricsCollectorSupplier = ingestMetricsCollectorSupplier;
    }

    @Override
    public DocumentSizeObserver newDocumentSizeObserver() {
        return new MeteringDocumentSizeObserver();
    }

    @Override
    public DocumentSizeObserver newFixedSizeDocumentObserver(long normalisedBytesParsed) {
        return new FixedDocumentSizeObserver(normalisedBytesParsed);
    }

    @Override
    public DocumentSizeReporter getDocumentParsingReporter() {
        return new MeteringDocumentSizeReporter(ingestMetricsCollectorSupplier.get());
    }
}
