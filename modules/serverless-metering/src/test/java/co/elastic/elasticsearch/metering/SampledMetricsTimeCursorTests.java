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

package co.elastic.elasticsearch.metering;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static co.elastic.elasticsearch.metering.SampledMetricsTimeCursor.generateSampleTimestamps;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SampledMetricsTimeCursorTests extends ESTestCase {

    public void testGenerateSampleTimestamps() {
        var now = Instant.now();
        TimeValue period = TimeValue.timeValueMinutes(5);
        List<Instant> timestamps = generateSampleTimestamps(now, Instant.MIN, period, 12).stream().toList();

        assertThat(timestamps, hasSize(12));
        assertThat(timestamps.get(0), equalTo(now));
        assertThat(timestamps.get(11), equalTo(now.minus(Duration.ofMinutes(55))));

        timestamps = generateSampleTimestamps(now, now, period, 12).stream().toList();

        assertThat(timestamps, empty());

        timestamps = generateSampleTimestamps(now, now.minus(Duration.ofMinutes(5)), period, 12).stream().toList();

        assertThat(timestamps, hasSize(1));
        assertThat(timestamps.get(0), equalTo(now));

        timestamps = generateSampleTimestamps(now, now.minus(Duration.ofMinutes(11)), period, 12).stream().toList();

        assertThat(timestamps, hasSize(3));
        assertThat(timestamps.get(0), equalTo(now));
        assertThat(timestamps.get(1), equalTo(now.minus(Duration.ofMinutes(5))));
        assertThat(timestamps.get(2), equalTo(now.minus(Duration.ofMinutes(10))));
    }

}
