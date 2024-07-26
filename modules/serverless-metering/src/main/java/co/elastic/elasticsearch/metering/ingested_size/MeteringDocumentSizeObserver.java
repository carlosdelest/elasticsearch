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

class MeteringDocumentSizeObserver implements DocumentSizeObserver, MeteringParser.NormalisedBytesParsedConsumer {
    private final boolean isUpdateByScript;
    private long normalisedBytesParsed = 0;

    MeteringDocumentSizeObserver(boolean isUpdateByScript) {
        this.isUpdateByScript = isUpdateByScript;
    }

    @Override
    public XContentParser wrapParser(XContentParser xContentParser) {
        return new MeteringParser(xContentParser, this);
    }

    @Override
    public long normalisedBytesParsed() {
        return normalisedBytesParsed;
    }

    public void normalisedBytesParsed(long value) {
        normalisedBytesParsed = value;
    }

    @Override
    public boolean isUpdateByScript() {
        return isUpdateByScript;
    }
}
