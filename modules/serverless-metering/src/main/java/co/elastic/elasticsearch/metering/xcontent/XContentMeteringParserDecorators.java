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

import org.elasticsearch.index.mapper.ParsedDocument.DocumentSize;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.xcontent.XContentParser;

class XContentMeteringParserDecorators {

    private XContentMeteringParserDecorators() {}

    /**
     * {@link XContentParserDecorator} reporting previously metered sizes (though, these are not recorded yet).
     *
     * <p>This implementation doesn't distinguish between ingested and stored bytes.
     * This is a noop decorator, no further metering will be done if parsing again.s
     */
    static class FixedSize implements XContentMeteringParserDecorator {
        private final DocumentSize normalizedSize;

        FixedSize(long bytesParsed) {
            this.normalizedSize = new DocumentSize(bytesParsed, bytesParsed);
        }

        @Override
        public DocumentSize meteredDocumentSize() {
            return normalizedSize;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return xContentParser;
        }
    }

    /**
     * {@link XContentParserDecorator} reporting the previously metered ingest size.
     * Further parsing will meter the stored size independently.
     */
    static class FixedIngestSize implements XContentMeteringParserDecorator {
        private DocumentSize normalizedSize;

        FixedIngestSize(long ingestedBytes) {
            this.normalizedSize = new DocumentSize(ingestedBytes, -1);
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return new XContentMeteringParser(
                xContentParser,
                bytesParsed -> normalizedSize = new DocumentSize(normalizedSize.ingestedBytes(), bytesParsed)
            );
        }

        @Override
        public DocumentSize meteredDocumentSize() {
            return normalizedSize;
        }
    }

    /**
     * Default {@link XContentParserDecorator} that meters the size of the document being parsed.
     *
     * <p>This implementation doesn't distinguish between ingested and stored bytes.
     */
    static class DefaultMetering implements XContentMeteringParserDecorator {
        private DocumentSize normalizedSize = DocumentSize.UNKNOWN;

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return new XContentMeteringParser(xContentParser, bytesParsed -> normalizedSize = new DocumentSize(bytesParsed, bytesParsed));
        }

        @Override
        public DocumentSize meteredDocumentSize() {
            return normalizedSize;
        }
    }
}
