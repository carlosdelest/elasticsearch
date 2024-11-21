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
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.verify;

public class CompositeDocumentSizeReporterTests extends ESTestCase {

    public void testDelegation() {
        DocumentSizeReporter mockReporter1 = Mockito.mock(DocumentSizeReporter.class);
        DocumentSizeReporter mockReporter2 = Mockito.mock(DocumentSizeReporter.class);
        Collection<DocumentSizeReporter> reporters = Arrays.asList(mockReporter1, mockReporter2);

        CompositeDocumentSizeReporter compositeReporter = new CompositeDocumentSizeReporter(reporters);

        ParsedDocument parsedDocument = Mockito.mock(ParsedDocument.class);

        compositeReporter.onParsingCompleted(parsedDocument);
        compositeReporter.onIndexingCompleted(parsedDocument);

        verify(mockReporter1).onParsingCompleted(parsedDocument);
        verify(mockReporter2).onParsingCompleted(parsedDocument);
        verify(mockReporter1).onIndexingCompleted(parsedDocument);
        verify(mockReporter2).onIndexingCompleted(parsedDocument);
    }
}
