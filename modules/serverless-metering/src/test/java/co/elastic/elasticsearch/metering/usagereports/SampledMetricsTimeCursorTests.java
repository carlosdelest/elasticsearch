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

import co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.Timestamps;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.NoSuchElementException;

import static co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.generateSampleTimestamps;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SampledMetricsTimeCursorTests extends ESTestCase {

    static final TimeValue PERIOD = TimeValue.timeValueMinutes(5);
    static final Duration PERIOD_DURATION = Duration.ofMinutes(5);

    public void testEmptyTimestamps() {
        var current = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        var timestamps = randomBoolean() ? Timestamps.EMPTY : generateSampleTimestamps(current, current, PERIOD);

        assertThat(timestamps.hasNext(), is(false));
        assertThat(timestamps.size(), is(0));
        assertThat(timestamps.limit(randomNonNegativeInt()), is(Timestamps.EMPTY));

        expectThrows(NoSuchElementException.class, timestamps::current);
        expectThrows(NoSuchElementException.class, timestamps::until);
        expectThrows(NoSuchElementException.class, timestamps::next);
    }

    public void testSingleTimestamps() {
        var current = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        var timestamps = randomBoolean()
            ? Timestamps.single(current)
            : generateSampleTimestamps(current, current.minus(PERIOD_DURATION), PERIOD);

        assertThat(timestamps.size(), is(1));
        assertThat(timestamps.limit(0), is(Timestamps.EMPTY));
        assertThat(timestamps.limit(1 + randomNonNegativeInt()), is(timestamps));

        assertThat(timestamps.current(), is(current));
        assertThat(timestamps.until(), is(current));
        assertThat(timestamps.hasNext(), is(true));
        assertThat(timestamps.next(), is(current));
        assertThat(timestamps.hasNext(), is(false));

        timestamps.reset();
        assertThat(timestamps.hasNext(), is(true));
        assertThat(timestamps.next(), is(current));

        expectThrows(NoSuchElementException.class, timestamps::next);
    }

    public void testGenerateSampleTimestamps() {
        var current = Instant.now().truncatedTo(ChronoUnit.MINUTES);

        var timestamps = generateSampleTimestamps(current, current.minus(Duration.ofHours(1)), PERIOD);
        assertThat(timestamps.current(), is(current));
        assertThat(timestamps.until(), is(current.minus(Duration.ofHours(1))));
        assertThat(timestamps.size(), is(12));
        assertThat(timestamps.limit(0), is(Timestamps.EMPTY));
        assertThat(timestamps.limit(12 + randomNonNegativeInt()), is(timestamps));

        var timestampList = Iterators.toList(timestamps);
        assertThat(timestampList, hasSize(12));
        assertThat(timestampList.get(0), is(timestamps.current()));
        assertThat(timestampList.get(1), is(timestamps.current().minus(PERIOD_DURATION)));
        assertThat(timestampList.get(11), is(timestamps.until().plus(PERIOD_DURATION)));

        assertThat(timestamps.hasNext(), is(false));
        timestamps.reset();
        assertThat(timestampList, equalTo(Iterators.toList(timestamps)));

        var limitedTimestamps = timestamps.limit(6);
        assertThat(limitedTimestamps.current(), is(current));
        assertThat(limitedTimestamps.until(), is(current.minus(Duration.ofMinutes(30))));
        assertThat(limitedTimestamps.size(), is(6));
        assertThat(limitedTimestamps.limit(6 + randomNonNegativeInt()), is(limitedTimestamps));

        assertThat(timestampList.subList(0, 6), equalTo(Iterators.toList(limitedTimestamps)));
    }
}
