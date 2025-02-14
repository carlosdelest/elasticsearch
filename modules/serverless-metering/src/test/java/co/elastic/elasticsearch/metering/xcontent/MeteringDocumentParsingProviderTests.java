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
import co.elastic.elasticsearch.metering.reporter.RawIngestMetricReporter;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.instanceOf;

public class MeteringDocumentParsingProviderTests extends ESTestCase {

    IngestMetricsProvider ingestMetricsProvider = Mockito.mock();

    public void testIndexIsReported() {
        String testUserIndex = "testIndex";
        MapperService mapperService = Mockito.mock(MapperService.class);

        MeteringDocumentParsingProvider meteringDocumentParsingProvider = new MeteringDocumentParsingProvider(
            false,
            () -> ingestMetricsProvider
        );

        DocumentSizeReporter documentSizeReporter = meteringDocumentParsingProvider.newDocumentSizeReporter(
            testUserIndex,
            mapperService,
            DocumentSizeAccumulator.EMPTY_INSTANCE
        );
        assertThat(documentSizeReporter, instanceOf(RawIngestMetricReporter.class));
    }
}
