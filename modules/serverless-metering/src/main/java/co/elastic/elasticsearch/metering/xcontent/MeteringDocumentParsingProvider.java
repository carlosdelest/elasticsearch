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
import co.elastic.elasticsearch.metering.reporter.CompositeDocumentSizeReporter;
import co.elastic.elasticsearch.metering.reporter.RAIngestMetricReporter;
import co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator;
import co.elastic.elasticsearch.metering.reporter.RAStorageReporter;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.xcontent.XContentParser;

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
    public <T> XContentMeteringParserDecorator newMeteringParserDecorator(IndexRequest request) {
        return new XContentDefaultMeteringParserDecorator();
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

    /**
     * Default {@link XContentParserDecorator} that meters the size of the document being parsed.
     */
    private static class XContentDefaultMeteringParserDecorator implements XContentMeteringParserDecorator {
        private long normalizedSize = UNKNOWN_SIZE;

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return new XContentMeteringParser(xContentParser, bytesParsed -> normalizedSize = bytesParsed);
        }

        @Override
        public long meteredDocumentSize() {
            return normalizedSize;
        }
    }
}
