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

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

public class RAStorageReporter implements DocumentSizeReporter {
    private final DocumentSizeAccumulator documentSizeAccumulator;
    private final MapperService mapperService;

    public RAStorageReporter(DocumentSizeAccumulator documentSizeAccumulator, MapperService mapperService) {
        this.documentSizeAccumulator = documentSizeAccumulator;
        this.mapperService = mapperService;
    }

    @Override
    public void onIndexingCompleted(ParsedDocument parsedDocument) {
        var documentSizeObserver = parsedDocument.getDocumentSizeObserver();
        if (isTimeSeries()) {
            // per index storage
            documentSizeAccumulator.add(documentSizeObserver.normalisedBytesParsed());
        }
    }

    @Override
    public void onParsingCompleted(ParsedDocument parsedDocument) {
        if (isTimeSeries() == false) {
            // todo per doc storage
        }
    }

    private boolean isTimeSeries() {
        if (mapperService != null) {
            return mapperService.mappingLookup().isDataStreamTimestampFieldEnabled();
        }
        return false;
    }

}
