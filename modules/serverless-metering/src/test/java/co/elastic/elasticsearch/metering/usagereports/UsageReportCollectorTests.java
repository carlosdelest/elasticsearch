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
import co.elastic.elasticsearch.metering.usagereports.publisher.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageMetrics;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageSource;
import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider.BackfillSink;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider.BackfillStrategy;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.generateSampleTimestamps;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.MAX_BACKFILL_LOOKBACK;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.MAX_CONSTANT_BACKFILL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.MAX_JITTER_FACTOR;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_BACKFILL_TOTAL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_FAILED_TOTAL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_RETRIED_TOTAL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_SENT_TOTAL;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportCollector.METERING_REPORTS_TOTAL;
import static co.elastic.elasticsearch.metrics.SampledMetricsProvider.metricValues;
import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UsageReportCollectorTests extends ESTestCase {

    static final TimeValue REPORT_PERIOD = TimeValue.timeValueMinutes(5);
    static final Duration REPORT_PERIOD_DURATION = Duration.ofSeconds(REPORT_PERIOD.seconds());
    static final Duration MAX_JITTER = Duration.ofNanos((long) Math.ceil(REPORT_PERIOD_DURATION.toNanos() * MAX_JITTER_FACTOR));

    static final String PROJECT_ID = "projectId";
    static final String NODE_ID = "nodeId";

    static final MetricValue SAMPLE1 = new MetricValue("sample1", "sample", Map.of(), 1L, Instant.EPOCH);
    static final MetricValue SAMPLE2 = new MetricValue("sample2", "sample", Map.of(), 2L, Instant.EPOCH);
    static final MetricValue SAMPLE3 = new MetricValue("sample3", "sample", Map.of(), 3L, Instant.EPOCH);
    static final MetricValue COUNTER = new MetricValue("counter", "counter", Map.of(), 100L, Instant.EPOCH);

    private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
    private final Clock clock = mock(Clock.class, x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));
    private final MeteringUsageRecordPublisher publisher = mock();
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();

    private UsageReportCollector startCollector(
        List<CounterMetricsProvider> counters,
        List<SampledMetricsProvider> samplers,
        SampledMetricsTimeCursor timestampCursor
    ) {
        when(publisher.sendRecords(anyList())).thenReturn(true); // success
        UsageReportCollector collector = new UsageReportCollector(
            NODE_ID,
            PROJECT_ID,
            counters,
            samplers,
            timestampCursor,
            publisher,
            deterministicTaskQueue.getThreadPool(),
            deterministicTaskQueue.getThreadPool().generic(),
            REPORT_PERIOD,
            clock,
            meterRegistry
        );
        collector.start();
        return collector;
    }

    private SampledMetricsTimeCursor inMemoryTimeCursor(Instant instant) {
        return new InMemorySampledMetricsTimeCursor(instant);
    }

    private SampledMetricsTimeCursor inMemoryTimeCursor() {
        return inMemoryTimeCursor(previousSampleTime(clock.instant()));
    }

    public void testTimeToNextRun() {
        final var now = Instant.EPOCH;
        final var late = nextSampleTime(now).plus(2, ChronoUnit.MINUTES);
        final var nextSampleTimestamp = nextSampleTime(now);

        var timeToNextRun = UsageReportCollector.timeToNextRun(true, now, nextSampleTimestamp, REPORT_PERIOD_DURATION);
        assertThat(
            "During normal execution next run should be around next period (within jitter)",
            timeToNextRun,
            withinJitterInterval(REPORT_PERIOD_DURATION)
        );

        timeToNextRun = UsageReportCollector.timeToNextRun(true, late, nextSampleTimestamp, REPORT_PERIOD_DURATION);
        assertThat("When late, the next run should be scheduled immediately", timeToNextRun, equalTo(TimeValue.ZERO));

        timeToNextRun = UsageReportCollector.timeToNextRun(false, now, nextSampleTimestamp, REPORT_PERIOD_DURATION);
        assertThat(
            "When retrying, next run should be around period / 10 (within jitter)",
            timeToNextRun,
            withinJitterInterval(REPORT_PERIOD_DURATION.dividedBy(10))
        );

        timeToNextRun = UsageReportCollector.timeToNextRun(false, late, nextSampleTimestamp, REPORT_PERIOD_DURATION);
        assertThat(
            "When retrying, next run should be around period / 10 (within jitter) even when late",
            timeToNextRun,
            withinJitterInterval(REPORT_PERIOD_DURATION.dividedBy(10))
        );
    }

    public void testReportCollectionAlwaysRunsOrderly() {
        var backfillStrategy = mock(BackfillStrategy.class);

        var sampleProvider = mockedSampleProvider(backfillStrategy, SAMPLE1);
        var counterProvider = mockedCounterProvider(COUNTER);
        var timestampCursor = inMemoryTimeCursor();
        startCollector(List.of(counterProvider), List.of(sampleProvider), timestampCursor);

        Instant sampleTime = clock.instant();
        Instant end = sampleTime.plus(Duration.ofHours(48));
        int counterCommits = 0;

        while (sampleTime.isBefore(end)) {
            var now = advanceTimeAndRunCollection(sampleTime);

            verify(publisher).sendRecords(List.of(usageRecord(COUNTER, now), usageRecord(SAMPLE1, sampleTime)));
            verifyNoMoreInteractions(publisher);

            verify(counterProvider.getMetrics(), times(++counterCommits)).commit();
            assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(sampleTime));

            sampleTime = nextSampleTime(sampleTime);
        }

        // mock start & completion time so that resulting runtime exceeds next sample period
        var timeOfSlowRun = advanceTimeAndRunCollection(
            now -> when(clock.instant()).thenReturn(now, now.plus(REPORT_PERIOD_DURATION).plus(MAX_JITTER))
        );
        assertThat(clock.instant(), is(timeOfSlowRun.plus(REPORT_PERIOD_DURATION).plus(MAX_JITTER)));

        // we expect a 2nd run to be instantly scheduled
        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, timeOfSlowRun), usageRecord(SAMPLE1, sampleTime)));
        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, clock.instant()), usageRecord(SAMPLE1, nextSampleTime(sampleTime))));
        verifyNoMoreInteractions(publisher);

        verify(counterProvider.getMetrics(), times(counterCommits + 2)).commit();
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(nextSampleTime(sampleTime)));

        verifyNoInteractions(backfillStrategy);
    }

    public void testErroneousCounterProvider() {
        var sampleTime = clock.instant();
        var committedTime = previousSampleTime(sampleTime);
        var timestampCursor = inMemoryTimeCursor(committedTime);

        var counterProvider1 = mock(CounterMetricsProvider.class);
        when(counterProvider1.getMetrics()).thenThrow(new RuntimeException("getMetrics() failed"));
        var counterProvider2 = mockedCounterProvider(COUNTER);
        startCollector(
            shuffledList(List.of(counterProvider1, counterProvider2)),
            List.of(mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1)),
            timestampCursor
        );

        // due to failing sample provider 1 we are not going to advance the sample timestamp
        var now = advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(sampleTime)); // erroneous counter doesn't impact samples
        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, now), usageRecord(SAMPLE1, sampleTime))); // all available records send
    }

    public void testErroneousSampleProvider() {
        var sampleTime = clock.instant();
        var committedTime = previousSampleTime(sampleTime);
        var timestampCursor = inMemoryTimeCursor(committedTime);

        var sampleProvider1 = mock(SampledMetricsProvider.class);
        when(sampleProvider1.getMetrics()).thenThrow(new RuntimeException("getMetrics() failed"));
        var sampleProvider2 = mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1);
        startCollector(List.of(mockedCounterProvider(COUNTER)), shuffledList(List.of(sampleProvider1, sampleProvider2)), timestampCursor);

        // due to failing sample provider 1 we are not going to advance the sample timestamp
        var now = advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(committedTime));
        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, now))); // no samples of sample provider 2 included
    }

    public void testUnavailableSampleProvider() {
        var sampleTime = clock.instant();
        var committedTime = previousSampleTime(sampleTime);
        var timestampCursor = inMemoryTimeCursor(committedTime);

        var sampleProvider1 = mock(SampledMetricsProvider.class);
        when(sampleProvider1.getMetrics()).thenReturn(Optional.empty());
        var sampleProvider2 = mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1);
        startCollector(List.of(mockedCounterProvider(COUNTER)), shuffledList(List.of(sampleProvider1, sampleProvider2)), timestampCursor);

        // due to unready sample provider 1 we are not going to advance the sample timestamp again
        var now = advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(committedTime));
        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, now))); // no samples of sample provider 2 included
    }

    public void testAdvanceSampleTimestampWithoutSamples() {
        var sampleProvider = mockedSampleProvider(BackfillStrategy.NOOP); // ready, but no samples returned
        var counterProviders = randomBoolean() ? List.of(mockedCounterProvider(COUNTER)) : List.<CounterMetricsProvider>of();
        var timestampCursor = inMemoryTimeCursor();
        startCollector(counterProviders, List.of(sampleProvider), timestampCursor);

        var sampleTime = clock.instant();
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(previousSampleTime(sampleTime)));
        advanceTimeAndRunCollection(sampleTime);
        // despite no reported samples, the cursor should advance
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(sampleTime));
    }

    public void testSkipSampleProvidersIfTimestampCursorNotReady() {
        var sampleProvider = mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1);
        var counterProvider = mockedCounterProvider(COUNTER);
        var timestampCursor = mock(SampledMetricsTimeCursor.class);
        startCollector(List.of(counterProvider), List.of(sampleProvider), timestampCursor);

        when(timestampCursor.getLatestCommittedTimestamp()).thenReturn(Optional.empty());
        when(timestampCursor.generateSampleTimestamps(any(), any())).thenReturn(Timestamps.EMPTY);

        var now = advanceTimeAndRunCollection(clock.instant());

        verify(publisher).sendRecords(List.of(usageRecord(COUNTER, now)));
        verifyNoMoreInteractions(publisher);

        verify(counterProvider.getMetrics()).commit();
        verify(timestampCursor, never()).commitUpTo(any());
        verifyNoInteractions(sampleProvider);
    }

    public void testPublishingFailure() {
        var sampleProvider = mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1);
        var counterProvider = mockedCounterProvider(COUNTER);
        var timestampCursor = inMemoryTimeCursor();
        startCollector(List.of(counterProvider), List.of(sampleProvider), timestampCursor);

        if (randomBoolean()) {
            when(publisher.sendRecords(anyList())).thenReturn(false); // publishing failed
        } else {
            doThrow(new RuntimeException("publishing failed")).when(publisher).sendRecords(anyList());
        }

        var sampleTime = clock.instant();
        var committedTime = previousSampleTime(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(committedTime));

        long attempts = 0;
        Instant lastAttemptTime = null;

        while (lastAttemptTime == null || sampleTimestamp(lastAttemptTime).equals(sampleTime)) {
            var attemptTime = advanceTimeAndRunCollection(t -> {});
            if (lastAttemptTime != null) {
                assertThat("timely retry expected", attemptTime, withinRetryPeriod(lastAttemptTime));
            }
            // the cursor was still not advanced
            assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(committedTime));
            // counter metrics are not committed
            verify(counterProvider.getMetrics(), never()).commit();

            lastAttemptTime = attemptTime;
            attempts++;
        }

        // Validate observability metrics
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_TOTAL), contains(measurement(is(attempts))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_FAILED_TOTAL), contains(measurement(is(attempts))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_RETRIED_TOTAL), contains(measurement(is(attempts - 1))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_TOTAL), contains(measurement(is(1L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_SENT_TOTAL), empty());
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL), empty());
    }

    public void testCommitSampleTimestampFailure() {
        var sampleProvider = mockedSampleProvider(BackfillStrategy.NOOP, SAMPLE1);
        var counterProvider = mockedCounterProvider(COUNTER);
        var timestampCursor = mock(SampledMetricsTimeCursor.class);
        startCollector(List.of(counterProvider), List.of(sampleProvider), timestampCursor);

        var sampleTime = clock.instant();
        var committedTime = previousSampleTime(sampleTime);

        when(timestampCursor.getLatestCommittedTimestamp()).thenReturn(Optional.of(committedTime));
        when(timestampCursor.commitUpTo(any())).thenReturn(false); // new sample timestamp can't be commited
        when(timestampCursor.generateSampleTimestamps(any(), any())).then(
            i -> generateSampleTimestamps(i.getArgument(0), committedTime, REPORT_PERIOD)
        );

        int attempts = 0;
        Instant lastAttemptTime = null;

        while (lastAttemptTime == null || sampleTimestamp(lastAttemptTime).equals(sampleTime)) {
            var attemptTime = advanceTimeAndRunCollection(t -> {});
            if (lastAttemptTime != null) {
                assertThat("timely retry expected", attemptTime, withinRetryPeriod(lastAttemptTime));
            }
            // counter metrics are not affected
            verify(counterProvider.getMetrics(), times(++attempts)).commit();

            lastAttemptTime = attemptTime;
        }

        // Validate observability metrics
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_TOTAL), contains(measurement(is((long) attempts))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_RETRIED_TOTAL), contains(measurement(is(attempts - 1L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_TOTAL), contains(measurement(is(1L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_SENT_TOTAL), contains(measurement(is((long) attempts))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL), empty());
    }

    /**
     * On a node change, we will have no local state. Test that we are backfilling a few (2) samples anyway, to account for common
     * scenarios in which this happens (scaling and rolling upgrade).
     */
    public void testConstantBackfillIfNoPreviousSamples() {

        var backfillStrategy = mock(BackfillStrategy.class);
        doAnswer(v -> {
            BackfillSink sink = v.getArgument(2);
            sink.add(v.getArgument(0), v.getArgument(1));
            return null;
        }).when(backfillStrategy).constant(any(), any(), any());

        var lag = randomIntBetween(1, 10);
        var backfills = Math.min(lag, MAX_CONSTANT_BACKFILL); // backfills are capped
        var dropped = lag - backfills;

        var sampleTime = clock.instant();
        // revert commit time to trigger backfill
        var timestampCursor = inMemoryTimeCursor();
        timestampCursor.commitUpTo(sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(1 + lag)));

        startCollector(List.of(), List.of(mockedSampleProvider(backfillStrategy, SAMPLE1)), timestampCursor);

        advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(sampleTime));
        verify(backfillStrategy, times(backfills)).constant(any(), any(), any());
        verify(backfillStrategy, never()).interpolate(any(), any(), any(), any(), any(), any());

        var expectedRecords = Iterators.toList(
            Iterators.forRange(0, 1 + backfills, i -> usageRecord(SAMPLE1, sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(i))))
        );
        verify(publisher).sendRecords(expectedRecords);

        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_SENT_TOTAL), contains(measurement(is(1L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_TOTAL), contains(measurement(is(1L))));
        if (dropped > 0) {
            assertThat(
                getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL),
                contains(measurement(is((long) dropped)))
            );
        } else {
            assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL), empty());
        }
    }

    public void testSkipBackfillIfNoSamples() {
        var sampleTime = clock.instant();
        var commitTime = sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(1 + randomIntBetween(1, 10)));

        var timestampCursor = inMemoryTimeCursor(commitTime); // revert commit time to trigger backfill

        startCollector(List.of(), List.of(mock(SampledMetricsProvider.class)), timestampCursor);

        advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(commitTime));
        verifyNoInteractions(publisher);

        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_TOTAL), contains(measurement(is(1L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_SENT_TOTAL), empty());
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_TOTAL), empty());
    }

    public void testInterpolatedBackfillUsingPreviousSamples() {
        var backfillStrategy = mock(BackfillStrategy.class);
        doAnswer(v -> {
            BackfillSink sink = v.getArgument(5);
            sink.add(v.getArgument(1), v.getArgument(4));
            return null;
        }).when(backfillStrategy).interpolate(any(), any(), any(), any(), any(), any());

        var sampleProvider = mockedSampleProvider(backfillStrategy, SAMPLE1, SAMPLE2);
        var timestampCursor = inMemoryTimeCursor();
        startCollector(List.of(), List.of(sampleProvider), timestampCursor);

        var initTime = clock.instant();
        advanceTimeAndRunCollection(initTime); // one collection to build up local state
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(initTime));

        var sampleTime = nextSampleTime(initTime);

        // revert commit time by number of lag periods to trigger backfill
        var lag = randomIntBetween(1, (int) MAX_BACKFILL_LOOKBACK.dividedBy(REPORT_PERIOD_DURATION) * 2);
        timestampCursor.commitUpTo(sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(1 + lag)));

        // report an additional new sample (SAMPLE3)
        when(sampleProvider.getMetrics()).thenReturn(Optional.of(metricValues(List.of(SAMPLE1, SAMPLE2, SAMPLE3), backfillStrategy)));

        advanceTimeAndRunCollection(sampleTime);
        assertThat(timestampCursor.getLatestCommittedTimestamp(), isPresentWith(sampleTime));

        var backfills = Math.min(lag, (int) MAX_BACKFILL_LOOKBACK.dividedBy(REPORT_PERIOD_DURATION) - 1); // backfills are capped
        var dropped = (lag - backfills);
        verify(backfillStrategy, times(backfills * 2)).interpolate(any(), any(), any(), any(), any(), any());
        verify(backfillStrategy, never()).constant(any(), any(), any());

        var expectedRecords = Iterators.toList(
            Iterators.concat(
                Iterators.forRange(0, 1 + backfills, i -> usageRecord(SAMPLE1, sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(i)))),
                Iterators.forRange(0, 1 + backfills, i -> usageRecord(SAMPLE2, sampleTime.minus(REPORT_PERIOD_DURATION.multipliedBy(i)))),
                Iterators.single(usageRecord(SAMPLE3, sampleTime)) // not backfilled, SAMPLE3 is new and no previous state is recorded
            )
        );
        verify(publisher).sendRecords(expectedRecords);

        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_SENT_TOTAL), contains(measurement(is(2L))));
        assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_TOTAL), contains(measurement(is(1L))));
        if (dropped > 0) {
            assertThat(
                getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL),
                contains(measurement(is((long) dropped)))
            );
        } else {
            assertThat(getMeasurements(LONG_COUNTER, METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL), empty());
        }
    }

    private Instant advanceTimeAndRunCollection(Instant sampleTime) {
        return advanceTimeAndRunCollection(
            time -> assertThat(time, both(greaterThanOrEqualTo(sampleTime)).and(lessThan(nextSampleTime(sampleTime))))
        );
    }

    private Instant advanceTimeAndRunCollection(Consumer<Instant> timeConsumer) {
        deterministicTaskQueue.advanceTime();
        Instant now = clock.instant();
        timeConsumer.accept(now);
        deterministicTaskQueue.runAllRunnableTasks();
        return now;
    }

    private SampledMetricsProvider mockedSampleProvider(BackfillStrategy backfillStrategy, MetricValue... values) {
        var sampleProvider = mock(SampledMetricsProvider.class);
        when(sampleProvider.getMetrics()).thenReturn(Optional.of(metricValues(List.of(values), backfillStrategy)));
        return sampleProvider;
    }

    private CounterMetricsProvider mockedCounterProvider(MetricValue... values) {
        var counterProvider = mock(CounterMetricsProvider.class);
        var mockedValues = mock(CounterMetricsProvider.MetricValues.class);
        when(counterProvider.getMetrics()).thenReturn(mockedValues);
        when(mockedValues.iterator()).thenAnswer(v -> Iterators.forArray(values));
        return counterProvider;
    }

    private static UsageRecord usageRecord(MetricValue value, Instant timestamp) {
        return new UsageRecord(
            value.id() + ":" + PROJECT_ID + ":" + timestamp.truncatedTo(ChronoUnit.SECONDS),
            timestamp,
            new UsageMetrics(value.type(), null, value.value(), REPORT_PERIOD, null, value.usageMetadata()),
            new UsageSource("es-" + NODE_ID, PROJECT_ID, value.sourceMetadata())
        );
    }

    private static Instant sampleTimestamp(Instant instant) {
        return SampleTimestampUtils.calculateSampleTimestamp(instant, REPORT_PERIOD_DURATION);
    }

    private static Instant previousSampleTime(Instant sampleTime) {
        assertThat("Sample timestamp expected", sampleTime, is(sampleTimestamp(sampleTime)));
        return sampleTime.minus(REPORT_PERIOD_DURATION);
    }

    private static Instant nextSampleTime(Instant sampleTime) {
        assertThat("Sample timestamp expected", sampleTime, is(sampleTimestamp(sampleTime)));
        return sampleTime.plus(REPORT_PERIOD_DURATION);
    }

    private static Matcher<Instant> withinRetryPeriod(Instant previous) {
        return new FeatureMatcher<>(is(withinJitterInterval(REPORT_PERIOD_DURATION.dividedBy(10))), "elapsed time", "time") {
            @Override
            protected TimeValue featureValueOf(Instant actual) {
                return TimeValue.timeValueMillis(Duration.between(previous, actual).toMillis());
            }
        };
    }

    private static Matcher<TimeValue> withinJitterInterval(Duration period) {
        var min = TimeValue.timeValueMillis((long) Math.floor(period.toMillis() * (1 - MAX_JITTER_FACTOR)));
        var max = TimeValue.timeValueMillis((long) Math.ceil(period.toMillis() * (1 + MAX_JITTER_FACTOR)));
        return both(greaterThanOrEqualTo(min)).and(lessThanOrEqualTo(max));
    }

    private List<Measurement> getMeasurements(InstrumentType type, String... names) {
        List<Measurement> measurements = new ArrayList<>();
        for (String name : names) {
            measurements.addAll(meterRegistry.getRecorder().getMeasurements(type, name));
        }
        return Measurement.combine(measurements);
    }

    private static Matcher<Measurement> measurement(Matcher<Long> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "a measurement of", "value") {
            @Override
            protected Long featureValueOf(Measurement measurement) {
                return measurement.getLong();
            }
        };
    }

}
