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

import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.xcontent.XContentParser;

/**
 * A document observer that will is initialised with already metered (but not recorded) bytes.
 * it will not increase this value when used in further parsing.
 * Used when a parsing already was measured in IngestService
 */
public class FixedDocumentSizeObserver implements DocumentSizeObserver {

    private final long normalisedBytesParsed;

    public FixedDocumentSizeObserver(long normalisedBytesParsed) {
        this.normalisedBytesParsed = normalisedBytesParsed;
    }

    @Override
    public XContentParser wrapParser(XContentParser xContentParser) {
        return xContentParser;
    }

    @Override
    public long normalisedBytesParsed() {
        return normalisedBytesParsed;
    }

    @Override
    public boolean isUpdateByScript() {
        return false;
    }
}
