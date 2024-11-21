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

import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;

import java.util.Collection;

/**
 * This is a reporter that encloses other reporters and will be delegating the method calls
 * to all enclosed reporters.
 */
public class CompositeDocumentSizeReporter implements DocumentSizeReporter {

    private final Collection<DocumentSizeReporter> reporters;

    public CompositeDocumentSizeReporter(Collection<DocumentSizeReporter> reporters) {
        this.reporters = reporters;
    }

    @Override
    public void onParsingCompleted(ParsedDocument parsedDocument) {
        reporters.forEach(r -> r.onParsingCompleted(parsedDocument));
    }

    @Override
    public void onIndexingCompleted(ParsedDocument parsedDocument) {
        reporters.forEach((r -> r.onIndexingCompleted(parsedDocument)));
    }
}
