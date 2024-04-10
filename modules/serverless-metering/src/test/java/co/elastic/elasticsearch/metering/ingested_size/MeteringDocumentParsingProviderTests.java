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

import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class MeteringDocumentParsingProviderTests extends ESTestCase {

    IngestMetricsCollector ingestMetricsCollector = Mockito.mock();

    public void testSystemIndicesAreNotReported() {
        SystemIndices mockSystemIndices = Mockito.mock(SystemIndices.class);
        String testSystemIndex = ".test_system_index";
        Mockito.when(mockSystemIndices.isSystemName(testSystemIndex)).thenReturn(true);

        Supplier<SystemIndices> systemIndicesSupplier = () -> mockSystemIndices;
        MeteringDocumentParsingProvider meteringDocumentParsingProvider = new MeteringDocumentParsingProvider(
            () -> ingestMetricsCollector,
            systemIndicesSupplier
        );

        DocumentSizeReporter documentParsingReporter = meteringDocumentParsingProvider.getDocumentParsingReporter(testSystemIndex);
        assertThat(documentParsingReporter, sameInstance(DocumentSizeReporter.EMPTY_INSTANCE));
    }

    public void testUserIndexIsReported() {
        SystemIndices mockSystemIndices = Mockito.mock(SystemIndices.class);
        String testUserIndex = "testIndex";
        Mockito.when(mockSystemIndices.isSystemName(testUserIndex)).thenReturn(false);

        Supplier<SystemIndices> systemIndicesSupplier = () -> mockSystemIndices;
        MeteringDocumentParsingProvider meteringDocumentParsingProvider = new MeteringDocumentParsingProvider(
            () -> ingestMetricsCollector,
            systemIndicesSupplier
        );

        DocumentSizeReporter documentSizeReporter = meteringDocumentParsingProvider.getDocumentParsingReporter(testUserIndex);
        assertThat(documentSizeReporter, instanceOf(MeteringDocumentSizeReporter.class));
    }
}
