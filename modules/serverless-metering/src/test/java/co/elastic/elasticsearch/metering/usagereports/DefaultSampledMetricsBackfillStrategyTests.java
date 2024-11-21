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

import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy.INSTANCE;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.DECADES;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;
import static org.hamcrest.Matchers.equalTo;

public class DefaultSampledMetricsBackfillStrategyTests extends ESTestCase {

    public static final Instant TIME = Instant.parse("2023-01-01T00:10:00Z");

    public static final long UNSET = -1;

    AtomicLong expectedValue = new AtomicLong(UNSET);
    SampledMetricsProvider.BackfillSink sink = (sample, value, metadata, timestamp) -> {
        assertThat(value, equalTo(expectedValue.get()));
        assertTrue(expectedValue.compareAndSet(value, UNSET));
    };

    private static MetricValue metricValue(long value, Instant creationTime) {
        return new MetricValue("id", "type", null, null, value, creationTime);
    }

    private static MetricValue metricValue(long value) {
        return metricValue(value, null);
    }

    public void testConstantBackfill() {
        // accept if no creation date
        var value = metricValue(randomNonNegativeLong());
        expectedValue.set(value.value());
        INSTANCE.constant(value, TIME, sink);
        assertThat(expectedValue.get(), equalTo(UNSET));

        // accept if creation date equal or in the past
        value = metricValue(randomNonNegativeLong(), TIME.minusMillis(randomNonNegativeInt()));
        expectedValue.set(value.value());
        INSTANCE.constant(value, TIME, sink);
        assertThat(expectedValue.get(), equalTo(UNSET));
    }

    public void testNoConstantBackfillIfObjectNewer() {
        var value = metricValue(randomNonNegativeLong(), TIME.plusMillis(1 + randomNonNegativeInt()));
        // expect nothing to be emitted to the sink
        INSTANCE.constant(value, TIME, sink);
    }

    public void testInterpolate() {
        final var step = randomFrom(DECADES, YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS).getDuration();
        final var start = randomBoolean() ? Instant.now() : Instant.ofEpochMilli(randomNonNegativeLong());
        final var end = start.minus(step.multipliedBy(Math.min(1000, start.toEpochMilli() / step.toMillis())));

        var timestamp = start;
        while (end.isAfter(timestamp) == false) {
            // current timestamp should match the interpolated timestamp
            expectedValue.set(timestamp.toEpochMilli());
            INSTANCE.interpolate(start, metricValue(start.toEpochMilli()), end, metricValue(end.toEpochMilli()), timestamp, sink);
            timestamp = timestamp.minus(step);
        }
    }
}
