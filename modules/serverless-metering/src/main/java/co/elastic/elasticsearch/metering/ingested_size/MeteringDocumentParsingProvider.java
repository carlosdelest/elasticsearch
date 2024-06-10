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
import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

import java.util.List;
import java.util.function.Supplier;

public class MeteringDocumentParsingProvider implements DocumentParsingProvider {
    private final Supplier<IngestMetricsCollector> ingestMetricsCollectorSupplier;
    private final Supplier<SystemIndices> systemIndicesSupplier;
    private final ProjectType projectType;

    public MeteringDocumentParsingProvider(
        ProjectType projectType,
        Supplier<IngestMetricsCollector> ingestMetricsCollectorSupplier,
        Supplier<SystemIndices> systemIndicesSupplier
    ) {
        this.projectType = projectType;
        this.ingestMetricsCollectorSupplier = ingestMetricsCollectorSupplier;
        this.systemIndicesSupplier = systemIndicesSupplier;
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
    public DocumentSizeReporter newDocumentSizeReporter(
        String indexName,
        IndexMode indexMode,
        DocumentSizeAccumulator documentSizeAccumulator
    ) {
        if (isSystemIndex(indexName)) {
            return DocumentSizeReporter.EMPTY_INSTANCE;
        }
        if (projectType == ProjectType.OBSERVABILITY || projectType == ProjectType.SECURITY) {
            DocumentSizeReporter raStorageReporter = new RAStorageReporter(documentSizeAccumulator, indexMode);
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
        if (projectType == ProjectType.OBSERVABILITY || projectType == ProjectType.SECURITY) {
            return new RAStorageAccumulator();
        }
        return DocumentSizeAccumulator.EMPTY_INSTANCE;
    }
}
