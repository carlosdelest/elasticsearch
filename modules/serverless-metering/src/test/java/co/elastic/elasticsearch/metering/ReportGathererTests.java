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

import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.metering.ReportGatherer.MAX_JITTER_FACTOR;
import static co.elastic.elasticsearch.metering.ReportGatherer.calculateSampleTimestamp;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
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

    private static class RecordingMetricsCollector implements SampledMetricsCollector {

        private record RecordedMetric(long value, String id) {}

        AtomicReference<RecordedMetric> currentRecordedMetric = new AtomicReference<>();
        AtomicInteger invocations = new AtomicInteger();

        private volatile boolean failCollecting;

        public void setFailCollecting(boolean b) {
            failCollecting = b;
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

        void report(List<UsageRecord> records) {
            // read last metric and clear it
            var lastRecordedMetric = currentRecordedMetric.getAndSet(null);
            assertNotNull("There should be a pending recorded metric", lastRecordedMetric);

            // compare with last record
            assertThat(
                "ReportGatherer UsageRecord should match the last recorded metric",
                records,
                contains(
                    allOf(
                        transformedMatch(UsageRecord::id, startsWith(lastRecordedMetric.id)),
                        transformedMatch(x -> x.usage().quantity(), equalTo(lastRecordedMetric.value))
                    )
                )
            );
        }
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
            recorder::report,
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
            x -> {},
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
            x -> {},
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

    public void testGenerateSampleTimestamps() {
        var reportGatherer = new ReportGatherer(
            "nodeId",
            "projectId",
            List.of(),
            List.of(),
            new InMemorySampledMetricsTimeCursor(),
            x -> {},
            null,
            null,
            TimeValue.timeValueMinutes(5),
            mock(Clock.class)
        );

        var now = Instant.now();
        List<Instant> timestamps = reportGatherer.generateSampleTimestamps(Instant.MIN, now);

        assertThat(timestamps, hasSize(12));
        assertThat(timestamps.get(0), equalTo(now));
        assertThat(timestamps.get(11), equalTo(now.minus(Duration.ofMinutes(55))));

        timestamps = reportGatherer.generateSampleTimestamps(now, now);

        assertThat(timestamps, empty());

        timestamps = reportGatherer.generateSampleTimestamps(now.minus(Duration.ofMinutes(5)), now);

        assertThat(timestamps, hasSize(1));
        assertThat(timestamps.get(0), equalTo(now));

        timestamps = reportGatherer.generateSampleTimestamps(now.minus(Duration.ofMinutes(11)), now);

        assertThat(timestamps, hasSize(3));
        assertThat(timestamps.get(0), equalTo(now));
        assertThat(timestamps.get(1), equalTo(now.minus(Duration.ofMinutes(5))));
        assertThat(timestamps.get(2), equalTo(now.minus(Duration.ofMinutes(10))));
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
            recorder::report,
            threadPool,
            threadPool.generic(),
            reportPeriod,
            clock
        );

        when(clock.instant()).thenAnswer(x -> now.get());
        reportGatherer.start();

        assertThat(cursor.getLatestCommitedTimestamp(), is(initialTimestamp));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        assertThat(cursor.getLatestCommitedTimestamp(), is(firstTimestamp));
        deterministicTaskQueue.advanceTime();

        recorder.invocations.set(0);
        // mock collection failure
        recorder.setFailCollecting(true);

        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(recorder.invocations.get(), equalTo(1));
        // We did not advance the cursor
        assertThat(cursor.getLatestCommitedTimestamp(), is(firstTimestamp));

        reportGatherer.cancel();
    }
}
