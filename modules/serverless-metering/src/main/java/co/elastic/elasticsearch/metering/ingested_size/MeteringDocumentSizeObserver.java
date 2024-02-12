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

import java.util.concurrent.atomic.AtomicLong;

public class MeteringDocumentSizeObserver implements DocumentSizeObserver {
    private final AtomicLong counter;

    public MeteringDocumentSizeObserver() {
        this(0L);
    }

    protected MeteringDocumentSizeObserver(long initValue) {
        this.counter = new AtomicLong(initValue);
    }

    @Override
    public XContentParser wrapParser(XContentParser xContentParser) {
        return new MeteringParser(xContentParser, counter);
    }

    @Override
    public long normalisedBytesParsed() {
        return counter.get();
    }

}
