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
import static co.elastic.elasticsearch.metering.ReportGatherer.calculateSampleTimestamp;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
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

    private static class RecordingMetricsCollector implements SampledMetricsCollector, MeteringUsageRecordPublisher {

        private record RecordedMetric(long value, String id) {}

        AtomicReference<RecordedMetric> currentRecordedMetric = new AtomicReference<>();
        AtomicInteger invocations = new AtomicInteger();

        private volatile boolean failCollecting;
        private volatile boolean failReporting;
        private Set<Instant> lastRecordTimestamps = Set.of();

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
            var newRecord = new RecordedMetric(randomLong(), UUIDs.randomBase64UUID());

            // assert last metrics where taken by `report`
            var lastRecord = currentRecordedMetric.getAndSet(newRecord);
            assertNull("There should not be a pending recorded metric", lastRecord);

            // store and return new metrics
            return new MetricValue(newRecord.id(), "type1", Map.of(), Map.of(), newRecord.value);
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

        var recorder = new RecordingMetricsCollector();
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
            clock
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
            Clock.systemUTC()
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
            Clock.systemUTC()
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
        var recorder = new RecordingMetricsCollector();
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
            clock
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
        var recorder = new RecordingMetricsCollector();
        var reportPeriod = TimeValue.timeValueMinutes(5);
        var reportPeriodDuration = Duration.ofSeconds(reportPeriod.seconds());
        var firstTimestamp = Instant.EPOCH;
        var secondTimestamp = firstTimestamp.plus(reportPeriodDuration);
        var thirdTimestamp = secondTimestamp.plus(reportPeriodDuration);

        var clock = Mockito.mock(Clock.class);
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        final Supplier<Instant> now = () -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis());

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
            clock
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

        reportGatherer.cancel();
    }
}
