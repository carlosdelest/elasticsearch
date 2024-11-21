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

import co.elastic.elasticsearch.metering.IngestMetricsProvider;
import co.elastic.elasticsearch.metering.reporter.RAIngestMetricReporter;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class MeteringDocumentParsingProviderTests extends ESTestCase {

    IngestMetricsProvider ingestMetricsProvider = Mockito.mock();

    public void testSystemIndicesAreNotReported() {
        SystemIndices mockSystemIndices = Mockito.mock(SystemIndices.class);
        MapperService mapperService = Mockito.mock(MapperService.class);
        String testSystemIndex = ".test_system_index";
        Mockito.when(mockSystemIndices.isSystemName(testSystemIndex)).thenReturn(true);

        Supplier<SystemIndices> systemIndicesSupplier = () -> mockSystemIndices;
        MeteringDocumentParsingProvider meteringDocumentParsingProvider = new MeteringDocumentParsingProvider(
            false,
            () -> ingestMetricsProvider,
            systemIndicesSupplier
        );

        DocumentSizeReporter documentParsingReporter = meteringDocumentParsingProvider.newDocumentSizeReporter(
            testSystemIndex,
            mapperService,
            DocumentSizeAccumulator.EMPTY_INSTANCE
        );
        assertThat(documentParsingReporter, sameInstance(DocumentSizeReporter.EMPTY_INSTANCE));
    }

    public void testUserIndexIsReported() {
        SystemIndices mockSystemIndices = Mockito.mock(SystemIndices.class);
        String testUserIndex = "testIndex";
        Mockito.when(mockSystemIndices.isSystemName(testUserIndex)).thenReturn(false);
        MapperService mapperService = Mockito.mock(MapperService.class);

        Supplier<SystemIndices> systemIndicesSupplier = () -> mockSystemIndices;
        MeteringDocumentParsingProvider meteringDocumentParsingProvider = new MeteringDocumentParsingProvider(
            false,
            () -> ingestMetricsProvider,
            systemIndicesSupplier
        );

        DocumentSizeReporter documentSizeReporter = meteringDocumentParsingProvider.newDocumentSizeReporter(
            testUserIndex,
            mapperService,
            DocumentSizeAccumulator.EMPTY_INSTANCE
        );
        assertThat(documentSizeReporter, instanceOf(RAIngestMetricReporter.class));
    }
}
