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

package co.elastic.elasticsearch.metering.xcontent;

import co.elastic.elasticsearch.metering.IngestMetricsProvider;
import co.elastic.elasticsearch.metering.ingested_size.reporter.CompositeDocumentSizeReporter;
import co.elastic.elasticsearch.metering.ingested_size.reporter.RAIngestMetricReporter;
import co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageAccumulator;
import co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageReporter;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;

import java.util.List;
import java.util.function.Supplier;

public class MeteringDocumentParsingProvider implements DocumentParsingProvider {
    private final Supplier<IngestMetricsProvider> ingestMetricsCollectorSupplier;
    private final Supplier<SystemIndices> systemIndicesSupplier;
    private final boolean meterRaStorage;

    public MeteringDocumentParsingProvider(
        boolean meterRaStorage,
        Supplier<IngestMetricsProvider> ingestMetricsCollectorSupplier,
        Supplier<SystemIndices> systemIndicesSupplier
    ) {
        this.meterRaStorage = meterRaStorage;
        this.ingestMetricsCollectorSupplier = ingestMetricsCollectorSupplier;
        this.systemIndicesSupplier = systemIndicesSupplier;
    }

    @Override
    public <T> XContentMeteringParserDecorator newMeteringParserDecorator(DocWriteRequest<T> request) {
        if (request instanceof IndexRequest indexRequest) {
            return newDocumentSizeObserver(indexRequest);
        } else if (request instanceof UpdateRequest updateRequest) {
            return newDocumentSizeObserver(updateRequest);
        }
        return XContentMeteringParserDecorator.NOOP;
    }

    private XContentMeteringParserDecorator newDocumentSizeObserver(IndexRequest indexRequest) {
        if (indexRequest.originatesFromUpdateByScript()) {
            // if required, meter stored size based on parsing of final document, but don't report ingest size
            return meterRaStorage ? new XContentMeteringParserDecorators.FixedIngestSize(0) : XContentMeteringParserDecorator.NOOP;
        } else if (indexRequest.originatesFromUpdateByDoc() && meterRaStorage) {
            // meter stored size based on parsing of final document, but report ingest size as metered previously
            return new XContentMeteringParserDecorators.FixedIngestSize(indexRequest.getNormalisedBytesParsed());
        } else if (indexRequest.getNormalisedBytesParsed() >= 0) {
            // report previously metered size both as ingest and stored size
            return new XContentMeteringParserDecorators.FixedSize(indexRequest.getNormalisedBytesParsed());
        }
        // no metering / parsing was previously done, use the default metering size observer
        return new XContentMeteringParserDecorators.DefaultMetering();
    }

    protected XContentMeteringParserDecorator newDocumentSizeObserver(UpdateRequest updateRequest) {
        if (updateRequest.doc() != null) {
            // meter the partial document for updates by document
            return new XContentMeteringParserDecorators.DefaultMetering();
        }
        // in case of updates by script, metering will be done on the resulting IndexRequest (if required)
        return XContentMeteringParserDecorator.NOOP;
    }

    @Override
    public DocumentSizeReporter newDocumentSizeReporter(
        String indexName,
        MapperService mapperService,
        DocumentSizeAccumulator documentSizeAccumulator
    ) {
        if (isSystemIndex(indexName)) {
            return DocumentSizeReporter.EMPTY_INSTANCE;
        }
        if (meterRaStorage) {
            DocumentSizeReporter raStorageReporter = new RAStorageReporter(documentSizeAccumulator, mapperService);
            DocumentSizeReporter raIngestReporter = new RAIngestMetricReporter(indexName, ingestMetricsCollectorSupplier.get());
            return new CompositeDocumentSizeReporter(List.of(raStorageReporter, raIngestReporter));
        }
        return new RAIngestMetricReporter(indexName, ingestMetricsCollectorSupplier.get());
    }

    private boolean isSystemIndex(String indexName) {
        assert systemIndicesSupplier.get() != null;
        return systemIndicesSupplier.get().isSystemName(indexName);
    }

    @Override
    public DocumentSizeAccumulator createDocumentSizeAccumulator() {
        if (meterRaStorage) {
            return new RAStorageAccumulator();
        }
        return DocumentSizeAccumulator.EMPTY_INSTANCE;
    }
}
