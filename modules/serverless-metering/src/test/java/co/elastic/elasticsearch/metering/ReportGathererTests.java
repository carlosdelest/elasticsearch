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

import co.elastic.elasticsearch.metering.reports.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.LambdaMatchers;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.ReportGatherer.MAX_JITTER_FACTOR;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.when;

public class ReportGathererTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    private static class TestRecordingMetricsCollector implements SampledMetricsCollector, MeteringUsageRecordPublisher {

        private final Supplier<String> metricIdProvider;
        private final Instant meteredObjectCreationDate;

        private record TestRecordedMetric(long value, String id) {}

        AtomicReference<TestRecordedMetric> currentRecordedMetric = new AtomicReference<>();
        AtomicInteger invocations = new AtomicInteger();

        private volatile boolean failCollecting;
        private volatile boolean failReporting;
        private Set<Instant> lastRecordTimestamps = Set.of();

        TestRecordingMetricsCollector(Supplier<String> metricIdProvider, Instant meteredObjectCreationDate) {
            this.metricIdProvider = metricIdProvider;
            this.meteredObjectCreationDate = meteredObjectCreationDate;
        }

        TestRecordingMetricsCollector() {
            this(randomConstantIdProvider(), Instant.EPOCH);
        }

        public static Supplier<String> randomConstantIdProvider() {
            return new Supplier<>() {
                private final String metricId = UUIDs.randomBase64UUID();

                @Override
                public String get() {
                    return metricId;
                }
            };
        }

        public void setFailCollecting(boolean b) {
            failCollecting = b;
        }

        public void setFailReporting(boolean b) {
            failReporting = b;
        }

        @Override
        public Optional<MetricValues> getMetrics() {
            invocations.incrementAndGet();
            if (failCollecting) {
                throw new RuntimeException("Metrics collection failure");
            }
            return Optional.of(SampledMetricsCollector.valuesFromCollection(List.of(generateAndRecordSingleMetric())));
        }

        private MetricValue generateAndRecordSingleMetric() {
            var newRecord = new TestRecordedMetric(randomLong(), metricIdProvider.get());

            // assert last metrics where taken by `report`
            var lastRecord = currentRecordedMetric.getAndSet(newRecord);
            assertNull("There should not be a pending recorded metric", lastRecord);

            // store and return new metrics
            return new MetricValue(newRecord.id(), "type1", Map.of(), newRecord.value, meteredObjectCreationDate);
        }

        @Override
        public void sendRecords(List<UsageRecord> records) throws IOException {
            // read last metric and clear it
            var lastRecordedMetric = currentRecordedMetric.getAndSet(null);
            assertNotNull("There should be a pending recorded metric", lastRecordedMetric);

            // compare with last record
            assertThat(
                "ReportGatherer UsageRecord should match the last recorded metric",
                records,
                hasItem(
                    allOf(
                        transformedMatch(UsageRecord::id, startsWith(lastRecordedMetric.id)),
                        LambdaMatchers.<UsageRecord, Long>transformedMatch(x -> x.usage().quantity(), equalTo(lastRecordedMetric.value))
                    )
                )
            );

            lastRecordTimestamps = records.stream().map(UsageRecord::usageTimestamp).collect(Collectors.toSet());

            if (failReporting) {
                throw new IOException("Report sending failure");
            }
        }

        @Override
        public void close() {}
    }

    public void testReportGatheringAlwaysRunsOrderly() {

        var recorder = new TestRecordingMetricsCollector();
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        Instant lower = Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
        Instant end = lower.plus(Duration.ofHours(48));

        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            new InMemorySampledMetricsTimeCursor(lower.minus(reportPeriodDuration)),
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            MeterRegistry.NOOP
        );

        when(clock.instant()).thenReturn(Instant.EPOCH);
        reportGatherer.start();

        while (lower.isBefore(end)) {
            deterministicTaskQueue.advanceTime();
            Instant now = Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
            when(clock.instant()).thenReturn(now);
            assertThat(now, both(greaterThanOrEqualTo(lower)).and(lessThan(lower.plus(reportPeriodDuration))));

            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(recorder.invocations.get(), equalTo(1));

            lower = lower.plus(reportPeriodDuration);
            recorder.invocations.set(0);
        }

        deterministicTaskQueue.advanceTime();
        Instant now = Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
        // mock start & completion time so that resulting runtime exceeds report period
        Duration maxJitter = Duration.ofNanos((long) Math.ceil(reportPeriodDuration.toNanos() * MAX_JITTER_FACTOR));
        when(clock.instant()).thenReturn(now, now.plus(reportPeriodDuration).plus(maxJitter));

        // we expect a 2nd run to be instantly scheduled
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(2));

        reportGatherer.cancel();
    }

    public void testCancelIdempotent() {

        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(),
            new InMemorySampledMetricsTimeCursor(),
            MeteringUsageRecordPublisher.NOOP_REPORTER,
            threadPool,
            threadPool.generic(),
            TimeValue.timeValueSeconds(1),
            Clock.systemUTC(),
            MeterRegistry.NOOP
        );

        reportGatherer.start();

        boolean cancelledOnce = reportGatherer.cancel();
        boolean cancelledTwice = reportGatherer.cancel();

        // Calling cancel a first time should report cancelled=true. Calling a second time should report cancelled=false
        // and have no other effect (no errors - do not throw)
        assertTrue("Gathering should have been cancelled", cancelledOnce);
        assertFalse("Calling cancel on an already cancelled ReportGatherer should have no effect", cancelledTwice);
    }

    public void testCancelCancels() {
        var deterministicTaskQueue = new DeterministicTaskQueue();

        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(),
            new InMemorySampledMetricsTimeCursor(),
            MeteringUsageRecordPublisher.NOOP_REPORTER,
            deterministicTaskQueue.getThreadPool(),
            deterministicTaskQueue.getThreadPool().generic(),
            TimeValue.timeValueMinutes(1),
            Clock.systemUTC(),
            MeterRegistry.NOOP
        );

        reportGatherer.start();

        assertThat(
            "A first gatherReports task should have been scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );

        deterministicTaskQueue.advanceTime();

        assertThat(
            "There should be a gatherReports task ready to run",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(false))
            )
        );

        deterministicTaskQueue.runRandomTask();

        assertThat(
            "A second gatherReports task should have been scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );

        var cancelled = reportGatherer.cancel();

        assertTrue("Gathering should have been cancelled", cancelled);

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runRandomTask();

        assertThat(
            "There should be no gatherReports task scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(false))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );
    }

    public void testFailingSampledCollectorDoesNotAdvanceTimestamp() {
        var recorder = new TestRecordingMetricsCollector();
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());

        var initialTimestamp = Instant.EPOCH.minus(reportPeriodDuration);
        var firstTimestamp = Instant.EPOCH;

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
        var cursor = new InMemorySampledMetricsTimeCursor(initialTimestamp);

        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            cursor,
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            MeterRegistry.NOOP
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        assertThat(cursor.getLatestCommittedTimestamp(), isPresentWith(initialTimestamp));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(cursor.getLatestCommittedTimestamp(), isPresentWith(firstTimestamp));
        deterministicTaskQueue.advanceTime();

        recorder.invocations.set(0);
        // mock collection failure
        recorder.setFailCollecting(true);

        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        // We did not advance the cursor
        assertThat(cursor.getLatestCommittedTimestamp(), isPresentWith(firstTimestamp));

        reportGatherer.cancel();
    }

    public void testTimeToNextRun() {
        var reportPeriodDuration = Duration.ofMinutes(5);

        var now = Instant.EPOCH;
        var nextSampleTimestamp = Instant.EPOCH.plus(reportPeriodDuration);
        var timeToNextRun = ReportGatherer.timeToNextRun(true, now, nextSampleTimestamp, reportPeriodDuration);

        assertThat(
            "During normal execution next run should be around next period (within jitter)",
            timeToNextRun.getMillis(),
            both(greaterThanOrEqualTo((long) (reportPeriodDuration.toMillis() * 0.7))).and(
                lessThanOrEqualTo((long) (reportPeriodDuration.toMillis() * 1.3))
            )
        );

        now = Instant.EPOCH.plus(reportPeriodDuration).plus(2, ChronoUnit.MINUTES);
        nextSampleTimestamp = Instant.EPOCH.plus(reportPeriodDuration);
        timeToNextRun = ReportGatherer.timeToNextRun(true, now, nextSampleTimestamp, reportPeriodDuration);

        assertThat("When late, the next run should be scheduled immediately", timeToNextRun.getMillis(), equalTo(0L));

        now = Instant.EPOCH.plus(2, ChronoUnit.MINUTES);
        nextSampleTimestamp = Instant.EPOCH.plus(reportPeriodDuration);
        timeToNextRun = ReportGatherer.timeToNextRun(false, now, nextSampleTimestamp, reportPeriodDuration);

        assertThat(
            "When retrying, next run should be around period / 10 (within jitter)",
            timeToNextRun.getMillis(),
            both(greaterThanOrEqualTo((long) (reportPeriodDuration.dividedBy(10).toMillis() * 0.7))).and(
                lessThanOrEqualTo((long) (reportPeriodDuration.dividedBy(10).toMillis() * 1.3))
            )
        );

        now = Instant.EPOCH.plusMillis(1000);
        nextSampleTimestamp = Instant.EPOCH;
        timeToNextRun = ReportGatherer.timeToNextRun(false, now, nextSampleTimestamp, reportPeriodDuration);

        assertThat(
            "When retrying, next run should be around period / 10 (within jitter) even when late",
            timeToNextRun.getMillis(),
            both(greaterThanOrEqualTo((long) (reportPeriodDuration.dividedBy(10).toMillis() * 0.7))).and(
                lessThanOrEqualTo((long) (reportPeriodDuration.dividedBy(10).toMillis() * 1.3))
            )
        );
    }

    public void testRetryScheduledWhenReportSendingFails() {
        var recorder = new TestRecordingMetricsCollector();
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var firstTimestamp = Instant.EPOCH;
        var secondTimestamp = firstTimestamp.plus(reportPeriodDuration);
        var thirdTimestamp = secondTimestamp.plus(reportPeriodDuration);

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());

        var meterRegistry = new RecordingMeterRegistry();
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration)),
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            meterRegistry
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(recorder.lastRecordTimestamps, contains(firstTimestamp));
        deterministicTaskQueue.advanceTime();

        recorder.invocations.set(0);
        // mock transmission failure
        recorder.setFailReporting(true);

        // we expect a 2nd run to be scheduled within a shorter timeframe
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(recorder.lastRecordTimestamps, contains(secondTimestamp));

        var firstAttemptTime = now.get();
        deterministicTaskQueue.advanceTime();
        var nextAttemptTime = now.get();

        var expectedRetryInterval = reportPeriodDuration.dividedBy(10);
        assertThat(
            Duration.between(firstAttemptTime, nextAttemptTime).toMillis(),
            both(greaterThanOrEqualTo((long) (expectedRetryInterval.toMillis() * 0.7))).and(
                lessThanOrEqualTo((long) (expectedRetryInterval.toMillis() * 1.3))
            )
        );

        // Advance to include the next timestamp, still failing
        var lastAttemptTime = firstAttemptTime.plus(reportPeriodDuration).plus(2, ChronoUnit.MINUTES);
        deterministicTaskQueue.runTasksUpToTimeInOrder(lastAttemptTime.toEpochMilli());
        deterministicTaskQueue.advanceTime();
        nextAttemptTime = now.get();
        assertThat(
            Duration.between(lastAttemptTime, nextAttemptTime).toMillis(),
            lessThanOrEqualTo((long) (expectedRetryInterval.toMillis() * 1.3))
        );

        // We are now trying to transmit 2 time frames
        assertThat(recorder.lastRecordTimestamps, containsInAnyOrder(secondTimestamp, thirdTimestamp));

        final List<Measurement> runs = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_TOTAL)
        );
        final List<Measurement> retried = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_RETRIED_TOTAL)
        );
        final List<Measurement> sent = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_SENT_TOTAL)
        );
        final List<Measurement> backfilled = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_BACKFILL_TOTAL)
        );
        final List<Measurement> dropped = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_BACKFILL_DROPPED_TOTAL)
        );

        assertThat(runs, contains(transformedMatch(Measurement::getLong, greaterThan(1L))));
        assertThat(retried, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(sent, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(backfilled, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(dropped, empty());

        reportGatherer.cancel();
    }

    public void testNoBackfillingWhenMetricNotFoundInLocalStatus() {
        var recorder = new TestRecordingMetricsCollector(() -> "id" + randomAlphaOfLength(10), Instant.EPOCH);
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var firstTimestamp = Instant.EPOCH;

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());

        var timeCursor = new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration));
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            timeCursor,
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            MeterRegistry.NOOP
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(recorder.lastRecordTimestamps, contains(firstTimestamp));
        deterministicTaskQueue.advanceTime();

        recorder.invocations.set(0);
        // mock transmission failure to "pack up" more timeframes to re-send
        recorder.setFailReporting(true);

        // we expect a 2nd run to be scheduled within a shorter timeframe
        deterministicTaskQueue.runAllRunnableTasks();
        var firstAttemptTime = now.get();
        deterministicTaskQueue.advanceTime();

        // Advance to include multiple timeframes, still failing
        var lastAttemptTime = firstAttemptTime.plus(reportPeriodDuration).plus(13, ChronoUnit.MINUTES);
        deterministicTaskQueue.runTasksUpToTimeInOrder(lastAttemptTime.toEpochMilli());
        deterministicTaskQueue.advanceTime();

        // But since we have new metrics (metrics with new id), we transmit only the last (current) timeframe
        var lastTimestamp = SampleTimestampUtils.calculateSampleTimestamp(lastAttemptTime, reportPeriodDuration);
        assertThat(recorder.lastRecordTimestamps, contains(lastTimestamp));

        reportGatherer.cancel();
    }

    public void testBackfillingOnlyForMetricFoundInLocalStatus() {
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var firstTimestamp = Instant.EPOCH;
        var secondTimestamp = firstTimestamp.plus(reportPeriodDuration);

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());

        List<MetricValue> producedMetricValues = new ArrayList<>();
        List<UsageRecord> sentRecords = new ArrayList<>();

        var timeCursor = new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration));
        var meterRegistry = new RecordingMeterRegistry();
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(() -> Optional.of(producedMetricValues::iterator)),
            timeCursor,
            new MeteringUsageRecordPublisher() {
                @Override
                public void sendRecords(List<UsageRecord> records) {
                    sentRecords.clear();
                    sentRecords.addAll(records);
                }

                @Override
                public void close() {}
            },
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            meterRegistry
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();
        Instant creationDate = Instant.now();
        producedMetricValues.add(new MetricValue("id-index1", "type1", Map.of(), 50L, creationDate));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(sentRecords, hasSize(1));
        assertThat(sentRecords, contains(transformedMatch(UsageRecord::usageTimestamp, equalTo(firstTimestamp))));
        deterministicTaskQueue.advanceTime();

        sentRecords.clear();
        producedMetricValues.clear();
        producedMetricValues.add(new MetricValue("id-index1", "type1", Map.of(), 60L, creationDate));
        // Add a previously unknown metric
        producedMetricValues.add(new MetricValue("id-index2", "type1", Map.of(), 70L, creationDate));

        // Simulate a retry by manually rolling back the cursor
        timeCursor.commitUpTo(firstTimestamp.minus(reportPeriodDuration));
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(sentRecords, hasSize(3));
        var index1Records = sentRecords.stream().filter(x -> x.id().startsWith("id-index1")).toList();
        var index2Records = sentRecords.stream().filter(x -> x.id().startsWith("id-index2")).toList();

        // We have 2 interpolated records for the previously seen metric
        assertThat(
            index1Records,
            containsInAnyOrder(
                both(transformedMatch(UsageRecord::usageTimestamp, equalTo(firstTimestamp))).and(
                    transformedMatch(x -> x.usage().quantity(), equalTo(55L))
                ),
                both(transformedMatch(UsageRecord::usageTimestamp, equalTo(secondTimestamp))).and(
                    transformedMatch(x -> x.usage().quantity(), equalTo(60L))
                )
            )
        );
        // We only have one record, the most recent one, for the "new" metric
        assertThat(
            index2Records,
            contains(
                both(transformedMatch(UsageRecord::usageTimestamp, equalTo(secondTimestamp))).and(
                    transformedMatch(x -> x.usage().quantity(), equalTo(70L))
                )
            )
        );

        reportGatherer.cancel();
    }

    /**
     * On a node change, we will have no local status. Test that we are backfilling a few (2) samples anyway, to account for common
     * scenarios in which this happens (scaling and rolling upgrade).
     */
    public void testConstantLimitedBackfillingWhenNoLocalStatus() {
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var currentTimestamp = Instant.EPOCH;
        var startTime = Instant.EPOCH.minus(reportPeriodDuration.multipliedBy(10));
        var recorder = new TestRecordingMetricsCollector(TestRecordingMetricsCollector.randomConstantIdProvider(), startTime);

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
        var meterRegistry = new RecordingMeterRegistry();
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            new InMemorySampledMetricsTimeCursor(startTime),
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            meterRegistry
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(
            recorder.lastRecordTimestamps,
            containsInAnyOrder(
                currentTimestamp,
                currentTimestamp.minus(reportPeriodDuration),
                currentTimestamp.minus(reportPeriodDuration.multipliedBy(2))
            )
        );

        reportGatherer.cancel();
    }

    public void testConstantBackfillingOnlyAfterCreationDate() {
        var recorder = new TestRecordingMetricsCollector(
            TestRecordingMetricsCollector.randomConstantIdProvider(),
            Instant.EPOCH.minus(1, ChronoUnit.MINUTES)
        );

        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var currentTimestamp = Instant.EPOCH;

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());
        var meterRegistry = new RecordingMeterRegistry();
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration.multipliedBy(10))),
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            meterRegistry
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));

        // Only 1 sample this time, as the others are before the creation time
        assertThat(recorder.lastRecordTimestamps, contains(currentTimestamp));

        reportGatherer.cancel();
    }

    public void testBackfillingLimited() {
        var recorder = new TestRecordingMetricsCollector();
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var firstTimestamp = Instant.EPOCH;

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());

        var meterRegistry = new RecordingMeterRegistry();
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(recorder),
            new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration)),
            recorder,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock,
            meterRegistry
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(recorder.lastRecordTimestamps, contains(firstTimestamp));
        deterministicTaskQueue.advanceTime();

        recorder.invocations.set(0);
        // mock transmission failure to "pack up" more timeframes to re-send
        recorder.setFailReporting(true);

        // Advance to collect N + 1 frames
        var runUpTo = firstTimestamp.plus(reportPeriodDuration.multipliedBy(reportGatherer.maxPeriodsLookback + 2)).toEpochMilli();
        while (deterministicTaskQueue.getCurrentTimeMillis() < runUpTo) {
            deterministicTaskQueue.runAllRunnableTasks();
            deterministicTaskQueue.advanceTime();
        }

        // Check we actually try to transmit only N frames
        assertThat(recorder.lastRecordTimestamps, hasSize(reportGatherer.maxPeriodsLookback));

        final List<Measurement> backfilled = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_BACKFILL_TOTAL)
        );
        final List<Measurement> dropped = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ReportGatherer.METERING_REPORTS_BACKFILL_DROPPED_TOTAL)
        );
        assertThat(backfilled, contains(transformedMatch(Measurement::getLong, greaterThan(2000L))));
        assertThat(dropped, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));

        reportGatherer.cancel();
    }
}
