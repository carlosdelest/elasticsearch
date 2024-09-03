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

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.generateSampleTimestamps;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SampledMetricsTimeCursorTests extends ESTestCase {

    public void testGenerateSampleTimestamps() {
        var now = Instant.now();
        TimeValue period = TimeValue.timeValueMinutes(5);
        var timestamps = generateSampleTimestamps(now, Instant.MIN, period);
        List<Instant> timestampList = Iterators.toList(Iterators.limit(timestamps, 12));

        assertThat(timestampList, hasSize(12));
        assertThat(timestampList.get(0), equalTo(now));
        assertThat(timestampList.get(11), equalTo(now.minus(Duration.ofMinutes(55))));
        assertThat(timestamps.last(), equalTo(Instant.MIN));

        timestamps = generateSampleTimestamps(now, now, period);

        assertThat(timestamps.hasNext(), is(false));

        timestamps = generateSampleTimestamps(now, now.minus(Duration.ofMinutes(5)), period);
        timestampList = Iterators.toList(Iterators.limit(timestamps, 12));

        assertThat(timestampList, hasSize(1));
        assertThat(timestampList.get(0), equalTo(now));
        assertThat(timestamps.last(), equalTo(now.minus(Duration.ofMinutes(5))));

        timestamps = generateSampleTimestamps(now, now.minus(Duration.ofMinutes(11)), period);
        timestampList = Iterators.toList(Iterators.limit(timestamps, 12));

        assertThat(timestampList, hasSize(3));
        assertThat(timestampList.get(0), equalTo(now));
        assertThat(timestampList.get(1), equalTo(now.minus(Duration.ofMinutes(5))));
        assertThat(timestampList.get(2), equalTo(now.minus(Duration.ofMinutes(10))));
        assertThat(timestamps.last(), equalTo(now.minus(Duration.ofMinutes(11))));
    }
}
