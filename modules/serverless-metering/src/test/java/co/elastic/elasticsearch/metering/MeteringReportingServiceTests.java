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
import co.elastic.elasticsearch.metrics.CounterMetricsCollector;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeteringReportingServiceTests extends ESTestCase {
    private static final String PROJECT_ID = "projectId";
    private static final String NODE_ID = "nodeId";

    private static final TimeValue REPORT_PERIOD = TimeValue.timeValueSeconds(5);

    private TestThreadPool threadPool;

    private static class TestCounter implements CounterMetricsCollector {
        private final String id;
        private final Map<String, String> metadata;

        private final AtomicLong metricValue = new AtomicLong();

        private TestCounter(String id, Map<String, String> metadata) {
            this.id = id;
            this.metadata = metadata;
        }

        public void add(long value) {
            metricValue.addAndGet(value);
        }

        class TestCommitMetricValues implements MetricValues {

            private final MetricValue metric;
            private final long snapshotValue;

            TestCommitMetricValues() {
                this.snapshotValue = metricValue.get();
                this.metric = new MetricValue(id, "test", metadata, snapshotValue);
            }

            @Override
            public Iterator<MetricValue> iterator() {
                return Iterators.single(metric);
            }

            @Override
            public void commit() {
                long newSize = metricValue.addAndGet(-snapshotValue);
                assert newSize >= 0;
            }
        }

        @Override
        public MetricValues getMetrics() {
            return new TestCommitMetricValues();
        }
    }

    private static class TestSample implements SampledMetricsCollector {
        private final String id;
        private final Map<String, String> metadata;

        private long value;

        private TestSample(String id, Map<String, String> metadata) {
            this.id = id;
            this.metadata = metadata;
        }

        public void set(long value) {
            this.value = value;
        }

        @Override
        public Optional<MetricValues> getMetrics() {
            return Optional.of(SampledMetricsCollector.valuesFromCollection(List.of(new MetricValue(id, "test", metadata, value))));
        }
    }

    @Before
    public void setup() {
        threadPool = new TestThreadPool("meteringServiceTests");
    }

    private static List<UsageRecord> pollRecords(
        Queue<UsageRecord> records,
        int num,
        TimeValue timeout,
        DeterministicTaskQueue deterministicTaskQueue
    ) {
        List<UsageRecord> found = new ArrayList<>(num);

        long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        do {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            long finishTime = deterministicTaskQueue.getCurrentTimeMillis();
            var elapsed = finishTime - startTime;

            assertThat("Timed out waiting for records", elapsed, lessThanOrEqualTo(timeout.getMillis()));

            UsageRecord r;
            while ((r = records.poll()) != null) {
                found.add(r);
            }
        } while (found.size() < num);

        return found;
    }

    private static Matcher<Instant> multipleOfReportPeriod() {
        Duration reportDuration = Duration.ofNanos(REPORT_PERIOD.nanos());
        return transformedMatch(i -> Duration.between(i.truncatedTo(ChronoUnit.HOURS), i).toNanos() % reportDuration.toNanos(), is(0L));
    }

    private static void checkRecords(List<UsageRecord> records, Map<String, Long> counterRecords, Map<String, Long> sampledRecords) {
        assertThat(
            records,
            everyItem(
                allOf(
                    transformedMatch(r -> r.source().id(), equalTo("es-" + NODE_ID)),
                    transformedMatch(r -> r.source().instanceGroupId(), equalTo(PROJECT_ID))
                )
            )
        );
        assertThat(
            "Sampled records should have a timestamp at the start of the reporting period",
            records.stream().filter(r -> r.id().startsWith("sample")).toList(),
            everyItem(transformedMatch(UsageRecord::usageTimestamp, is(multipleOfReportPeriod())))
        );

        assertThat(
            records,
            containsInAnyOrder(
                Stream.<Matcher<? super UsageRecord>>concat(
                    counterRecords.entrySet()
                        .stream()
                        .map(
                            e -> allOf(
                                transformedMatch(r -> r.usage().type(), equalTo("test")),
                                transformedMatch(r -> r.usage().quantity(), equalTo(e.getValue())),
                                transformedMatch(r -> r.source().metadata(), equalTo(Map.of("id", e.getKey())))
                            )
                        ),
                    sampledRecords.entrySet()
                        .stream()
                        .map(
                            e -> allOf(
                                transformedMatch(r -> r.usage().type(), equalTo("test")),
                                transformedMatch(r -> r.usage().quantity(), equalTo(e.getValue())),
                                transformedMatch(r -> r.source().metadata(), equalTo(Map.of("id", e.getKey())))
                            )
                        )
                ).toList()
            )
        );
    }

    public void testServiceReportsMetrics() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var counter2 = new TestCounter("counter2", Map.of("id", "counter2"));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1, counter2),
                List.of(sampled1, sampled2),
                new InMemorySampledMetricsTimeCursor(Instant.EPOCH.minus(reportPeriodDuration)),
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            counter2.add(20);
            sampled1.set(50);
            sampled2.set(60);

            service.start();

            var reported = pollRecords(records, 4, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of("counter1", 15L, "counter2", 20L), Map.of("sampled1", 50L, "sampled2", 60L));

            service.stop();
        }
    }

    public void testCounterMetricsReset() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var counter2 = new TestCounter("counter2", Map.of("id", "counter2"));

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1, counter2),
                List.of(),
                new InMemorySampledMetricsTimeCursor(),
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {
            counter1.add(10);
            counter1.add(5);
            counter2.add(20);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of("counter1", 15L, "counter2", 20L), Map.of());

            // counters are reset after metrics are sent
            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of("counter1", 0L, "counter2", 0L), Map.of());

            counter1.add(5);
            counter2.add(10);

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of("counter1", 5L, "counter2", 10L), Map.of());

            service.stop();
        }
    }

    public void testSampledMetricsMaintained() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(),
                List.of(sampled1, sampled2),
                new InMemorySampledMetricsTimeCursor(Instant.EPOCH.minus(reportPeriodDuration)),
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {
            sampled1.set(50);
            sampled2.set(60);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            sampled1.set(20);
            sampled2.set(65);

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of(), Map.of("sampled1", 20L, "sampled2", 65L));

            service.stop();
        }
    }

    public void testMultipleRecordsNotBackfilledWhenMissingData() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var initialTime = Instant.EPOCH.minus(reportPeriodDuration.multipliedBy(2));
        var firstSampleTimestamp = initialTime.plus(reportPeriodDuration);
        var secondSampleTimestamp = firstSampleTimestamp.plus(reportPeriodDuration);

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        var cursor = new InMemorySampledMetricsTimeCursor(initialTime);

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                cursor,
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            sampled1.set(50);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);

            var counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            var sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(1));

            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(15L));
            assertThat(sampledMetrics.get(0).usageTimestamp(), equalTo(secondSampleTimestamp));

            assertThat(sampledMetrics.stream().map(usageRecord -> usageRecord.usage().quantity()).toList(), everyItem(is(50L)));
            service.stop();
        }
    }

    public void testMultipleRecordsSentUpToCommittedTimestampWithInterpolation() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var initialTime = Instant.EPOCH.minus(reportPeriodDuration.multipliedBy(2));
        var firstSampleTimestamp = initialTime.plus(reportPeriodDuration);
        var secondSampleTimestamp = firstSampleTimestamp.plus(reportPeriodDuration);
        var thirdSampleTimestamp = secondSampleTimestamp.plus(reportPeriodDuration);

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        var cursor = new InMemorySampledMetricsTimeCursor(initialTime);

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                cursor,
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            sampled1.set(50);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);

            var counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            var sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(1));

            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(15L));
            assertThat(sampledMetrics.get(0).usageTimestamp(), equalTo(secondSampleTimestamp));

            sampled1.set(150);
            // Simulate a "skipped" timestamp
            cursor.commitUpTo(firstSampleTimestamp);
            reported = pollRecords(records, 3, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(2));

            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(0L));
            assertThat(
                sampledMetrics,
                containsInAnyOrder(
                    allOf(
                        transformedMatch(UsageRecord::usageTimestamp, equalTo(secondSampleTimestamp)),
                        transformedMatch(usageRecord -> usageRecord.usage().quantity(), equalTo(100L))
                    ),
                    allOf(
                        transformedMatch(UsageRecord::usageTimestamp, equalTo(thirdSampleTimestamp)),
                        transformedMatch(x -> x.usage().quantity(), equalTo(150L))
                    )
                )
            );

            service.stop();
        }
    }

    public void testRecordsNotSentAgainWhenTimestampCommitted() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        final var initialTimestamp = Instant.EPOCH.minus(reportPeriodDuration);
        var firstSampleTimestamp = initialTimestamp.plus(reportPeriodDuration);

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        var cursor = new InMemorySampledMetricsTimeCursor(initialTimestamp);

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                cursor,
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            sampled1.set(50);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);

            var counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            var sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(1));

            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(15L));

            assertThat(sampledMetrics.get(0).usageTimestamp(), equalTo(firstSampleTimestamp));
            assertThat(sampledMetrics.get(0).usage().quantity(), equalTo(50L));

            cursor.commitUpTo(firstSampleTimestamp.plus(reportPeriodDuration.multipliedBy(2)));

            reported = pollRecords(records, 1, TimeValue.timeValueSeconds(10), deterministicTaskQueue);

            assertThat(records, empty());
            counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, empty());

            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(0L));

            service.stop();
        }
    }

    public void testSampledMetricsCalledConcurrentlyMatch() throws InterruptedException {

        final var results = new ConcurrentLinkedQueue<MetricValue>();

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);

        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"));

        final long valueForSampled1 = 50L;
        final long valueForSampled2 = 60L;

        final int totalOps = writeOpsPerThread * writerThreadsCount;
        final long expectedSum = valueForSampled1 * totalOps + valueForSampled2 * totalOps;

        List<SampledMetricsCollector> collectors = List.of(sampled1, sampled2);
        try (
            var ignored = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(),
                collectors,
                new InMemorySampledMetricsTimeCursor(),
                REPORT_PERIOD,
                MeteringUsageRecordPublisher.NOOP_REPORTER,
                threadPool,
                threadPool.generic(),
                MeterRegistry.NOOP
            )
        ) {
            sampled1.set(valueForSampled1);
            sampled2.set(valueForSampled2);

            // We mock manually ReportGatherer by
            // (1) NOT using the one embedded in MetricsService (by avoid calls to service.start/stop) and
            // (2) calling service.getMetrics() directly
            ConcurrencyTestUtils.runConcurrent(
                writerThreadsCount,
                writeOpsPerThread,
                () -> randomIntBetween(10, 50),
                () -> addSampledResults(results, collectors),
                logger::info
            );
        }

        long valueSum = results.stream().mapToLong(MetricValue::value).sum();
        assertThat(results.size(), equalTo(totalOps * 2));
        assertThat(valueSum, equalTo(expectedSum));
    }

    public void testCounterMetricsCalledConcurrentlyMatch() throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<MetricValue>();

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = 1;

        var counters = new TestCounter[] {
            new TestCounter("counter1", Map.of("id", "counter1")),
            new TestCounter("counter2", Map.of("id", "counter2")) };

        // Associate each writer thread to a counter it will use/increment
        final int[] writerThreadTargetTestCounter = IntStream.range(0, writerThreadsCount)
            .map(i -> randomIntBetween(0, counters.length - 1))
            .toArray();

        final long writerThreadCounterIncrement = randomLongBetween(10L, 100L);

        // The expected sum - computed as if all threads incremented their counters sequentially
        final long expectedSum = writerThreadsCount * writerThreadCounterIncrement * writeOpsPerThread;

        try (
            var ignored = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                Arrays.asList(counters),
                List.of(),
                new InMemorySampledMetricsTimeCursor(),
                REPORT_PERIOD,
                MeteringUsageRecordPublisher.NOOP_REPORTER,
                threadPool,
                threadPool.generic(),
                MeterRegistry.NOOP
            )
        ) {
            // We mock manually ReportGatherer by
            // (1) NOT using the one embedded in MetricsService (by avoid calls to service.start/stop) and
            // (2) calling service.getMetrics() directly
            ConcurrencyTestUtils.runConcurrentWithCollectors(writerThreadsCount, writeOpsPerThread, () -> randomIntBetween(10, 50), t -> {
                var counter = counters[writerThreadTargetTestCounter[t]];
                counter.add(writerThreadCounterIncrement);
            },
                collectThreadsCount,
                () -> randomIntBetween(100, 200),
                () -> addCounterResults(results, Arrays.asList(counters)),
                logger::info
            );
        }

        long valueSum = results.stream().mapToLong(MetricValue::value).sum();
        assertThat(valueSum, equalTo(expectedSum));
    }

    public void testStopStops() {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());

        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis()));

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                new InMemorySampledMetricsTimeCursor(Instant.EPOCH.minus(reportPeriodDuration)),
                REPORT_PERIOD,
                new TestMeteringUsageRecordPublisher(records),
                threadPool,
                threadPool.generic(),
                clock,
                MeterRegistry.NOOP
            )
        ) {
            counter1.add(15);
            sampled1.set(50);

            service.start();

            var reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10), deterministicTaskQueue);
            checkRecords(reported, Map.of("counter1", 15L), Map.of("sampled1", 50L));

            service.stop();

            counter1.add(20);
            sampled1.set(60);

            // no more records should appear
            var runUpToMillis = Duration.ofMillis(deterministicTaskQueue.getCurrentTimeMillis()).plusSeconds(10).toMillis();
            deterministicTaskQueue.runTasksUpToTimeInOrder(runUpToMillis);

            assertThat(records, empty());
        }
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static void addSampledResults(ConcurrentLinkedQueue<MetricValue> results, List<SampledMetricsCollector> collectors) {
        for (var collector : collectors) {
            var metrics = collector.getMetrics();
            assertThat(metrics, isPresent());
            metrics.orElseThrow(AssertionError::new).forEach(results::add);
        }
    }

    private static void addCounterResults(ConcurrentLinkedQueue<MetricValue> results, List<CounterMetricsCollector> collectors) {
        for (var collector : collectors) {
            var metrics = collector.getMetrics();
            metrics.forEach(results::add);
            metrics.commit();
        }
    }

    private record TestMeteringUsageRecordPublisher(Queue<UsageRecord> records) implements MeteringUsageRecordPublisher {

        @Override
        public void sendRecords(List<UsageRecord> records) {
            this.records.addAll(records);
        }

        @Override
        public void close() {}
    }
}
