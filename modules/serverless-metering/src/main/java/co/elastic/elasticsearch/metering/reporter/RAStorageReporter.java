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

import co.elastic.elasticsearch.metering.RaStorageMetadataFieldMapper;

import org.apache.lucene.document.NumericDocValuesField;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;

public class RAStorageReporter implements DocumentSizeReporter {
    private final DocumentSizeAccumulator documentSizeAccumulator;
    private final MapperService mapperService;

    public RAStorageReporter(DocumentSizeAccumulator documentSizeAccumulator, MapperService mapperService) {
        this.documentSizeAccumulator = documentSizeAccumulator;
        this.mapperService = mapperService;
    }

    @Override
    public void onIndexingCompleted(ParsedDocument parsedDocument) {
        long bytesToReport = parsedDocument.getNormalizedSize();
        if (isTimeSeries() && bytesToReport > XContentMeteringParserDecorator.UNKNOWN_SIZE) {
            // per index storage
            documentSizeAccumulator.add(bytesToReport);
        }
    }

    @Override
    public void onParsingCompleted(ParsedDocument parsedDocument) {
        long bytesToReport = parsedDocument.getNormalizedSize();
        if (isTimeSeries() == false && bytesToReport > XContentMeteringParserDecorator.UNKNOWN_SIZE) {
            // Store the result in a new, "hidden" field
            parsedDocument.rootDoc().add(new NumericDocValuesField(RaStorageMetadataFieldMapper.FIELD_NAME, bytesToReport));
        }
    }

    private boolean isTimeSeries() {
        if (mapperService != null) {
            return mapperService.mappingLookup().isDataStreamTimestampFieldEnabled();
        }
        return false;
    }
}
