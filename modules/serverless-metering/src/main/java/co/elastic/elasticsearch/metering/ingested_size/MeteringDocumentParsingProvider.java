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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.mapper.MapperService;
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
    public <T> DocumentSizeObserver newDocumentSizeObserver(DocWriteRequest<T> request) {
        if (request instanceof IndexRequest indexRequest) {
            return newDocumentSizeObserver(indexRequest);
        } else if (request instanceof UpdateRequest updateRequest) {
            return newDocumentSizeObserver(updateRequest);
        }
        return DocumentSizeObserver.EMPTY_INSTANCE;
    }

    private DocumentSizeObserver newDocumentSizeObserver(IndexRequest indexRequest) {
        if (indexRequest.getNormalisedBytesParsed() >= 0) {
            return new FixedDocumentSizeObserver(indexRequest.getNormalisedBytesParsed());
        } else if (indexRequest.originatesFromUpdateByScript() && isObservabilityOrSecurity() == false) {
            return DocumentSizeObserver.EMPTY_INSTANCE; // no metering necessary
        }
        // request.getNormalisedBytesParsed() -1, meaning normalisedBytesParsed isn't set as parsing wasn't done yet
        return new MeteringDocumentSizeObserver(indexRequest.originatesFromUpdateByScript());
    }

    private DocumentSizeObserver newDocumentSizeObserver(UpdateRequest updateRequest) {
        if (isUpdateByDoc(updateRequest)) {
            // meter the partial doc
            return new MeteringDocumentSizeObserver(false);
        }
        // update by script are metered on the IndexRequest resulting from execution of the script
        return DocumentSizeObserver.EMPTY_INSTANCE;
    }

    private static boolean isUpdateByDoc(UpdateRequest updateRequest) {
        return updateRequest.script() == null && updateRequest.doc() != null;
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
        if (isObservabilityOrSecurity()) {
            DocumentSizeReporter raStorageReporter = new RAStorageReporter(documentSizeAccumulator, mapperService);
            DocumentSizeReporter raIngestReporter = new RAIngestMetricReporter(indexName, ingestMetricsCollectorSupplier.get());
            return new CompositeDocumentSizeReporter(List.of(raStorageReporter, raIngestReporter));
        }
        return new RAIngestMetricReporter(indexName, ingestMetricsCollectorSupplier.get());
    }

    private boolean isObservabilityOrSecurity() {
        return projectType == ProjectType.OBSERVABILITY || projectType == ProjectType.SECURITY;
    }

    private boolean isSystemIndex(String indexName) {
        assert systemIndicesSupplier.get() != null;
        return systemIndicesSupplier.get().isSystemName(indexName);
    }

    @Override
    public DocumentSizeAccumulator createDocumentSizeAccumulator() {
        if (isObservabilityOrSecurity()) {
            return new RAStorageAccumulator();
        }
        return DocumentSizeAccumulator.EMPTY_INSTANCE;
    }
}
