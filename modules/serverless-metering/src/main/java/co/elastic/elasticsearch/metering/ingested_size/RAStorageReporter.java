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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

public class RAStorageReporter implements DocumentSizeReporter {
    private static final Logger logger = LogManager.getLogger(RAStorageReporter.class);
    private final DocumentSizeAccumulator documentSizeAccumulator;
    private final MapperService mapperService;

    public RAStorageReporter(DocumentSizeAccumulator documentSizeAccumulator, MapperService mapperService) {
        this.documentSizeAccumulator = documentSizeAccumulator;
        this.mapperService = mapperService;
    }

    @Override
    public void onIndexingCompleted(ParsedDocument parsedDocument) {
        if (isTimeSeries()) {
            // per index storage
            documentSizeAccumulator.add(parsedDocument.getDocumentSizeObserver().normalisedBytesParsed());
        }
    }

    @Override
    public void onParsingCompleted(ParsedDocument parsedDocument) {
        if (isTimeSeries() == false) {
            logger.trace("parsing completed for non time series index");
        }
    }

    private boolean isTimeSeries() {
        if (mapperService != null) {
            return mapperService.mappingLookup().isDataStreamTimestampFieldEnabled();
        }
        return false;
    }

}
