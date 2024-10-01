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

package co.elastic.elasticsearch.metering.usagereports;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;

import static co.elastic.elasticsearch.metering.usagereports.SampleTimestampUtils.calculateSampleTimestamp;
import static org.hamcrest.Matchers.equalTo;

public class SampleTimestampUtilsTests extends ESTestCase {

    public void testCalculateSampleTimestamp() {
        assertThat(
            calculateSampleTimestamp(Instant.parse("2023-01-01T12:00:00Z"), Duration.ofHours(1)),
            equalTo(Instant.parse("2023-01-01T12:00:00Z"))
        );
        assertThat(
            calculateSampleTimestamp(Instant.parse("2023-01-01T12:20:46Z"), Duration.ofMinutes(2)),
            equalTo(Instant.parse("2023-01-01T12:20:00Z"))
        );
        assertThat(
            calculateSampleTimestamp(Instant.parse("2023-01-01T12:22:00Z"), Duration.ofMinutes(2)),
            equalTo(Instant.parse("2023-01-01T12:22:00Z"))
        );
        assertThat(
            calculateSampleTimestamp(Instant.parse("2023-01-01T01:04:59Z"), Duration.ofMinutes(5)),
            equalTo(Instant.parse("2023-01-01T01:00:00Z"))
        );
        assertThat(
            calculateSampleTimestamp(Instant.parse("2023-01-01T01:04:59Z"), Duration.ofSeconds(15)),
            equalTo(Instant.parse("2023-01-01T01:04:45Z"))
        );

        expectThrows(AssertionError.class, () -> calculateSampleTimestamp(Instant.parse("2023-01-01T00:00:00Z"), Duration.ofHours(2)));
    }
}
