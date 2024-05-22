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
import co.elastic.elasticsearch.metrics.CounterMetricsCollector;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeteringReportingServiceTests extends ESTestCase {
    private static final String PROJECT_ID = "projectId";
    private static final String NODE_ID = "nodeId";

    private static final TimeValue REPORT_PERIOD = TimeValue.timeValueSeconds(5);

    private TestThreadPool threadPool;
    private Settings settings;
    private ClusterSettings clusterSettings;

    private static class TestCounter implements CounterMetricsCollector {
        private final String id;
        private final Map<String, String> metadata;
        private final Map<String, Object> settings;

        private final AtomicLong metricValue = new AtomicLong();

        private TestCounter(String id, Map<String, String> metadata, Map<String, Object> settings) {
            this.id = id;
            this.metadata = metadata;
            this.settings = settings;
        }

        public void add(long value) {
            metricValue.addAndGet(value);
        }

        class TestCommitMetricValues implements MetricValues {

            private final MetricValue metric;
            private final long snapshotValue;

            TestCommitMetricValues() {
                this.snapshotValue = metricValue.get();
                this.metric = new MetricValue(id, "test", metadata, settings, snapshotValue);
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
        private final Map<String, Object> settings;

        private long value;

        private TestSample(String id, Map<String, String> metadata, Map<String, Object> settings) {
            this.id = id;
            this.metadata = metadata;
            this.settings = settings;
        }

        public void set(long value) {
            this.value = value;
        }

        @Override
        public Optional<MetricValues> getMetrics() {
            return Optional.of(
                SampledMetricsCollector.valuesFromCollection(List.of(new MetricValue(id, "test", metadata, settings, value)))
            );
        }
    }

    @Before
    public void setup() {
        threadPool = new TestThreadPool("meteringServiceTests");
        settings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_ID.getKey(), PROJECT_ID)
            .put(MeteringReportingService.REPORT_PERIOD.getKey(), REPORT_PERIOD)
            .build();
        clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                ServerlessSharedSettings.PROJECT_ID,
                MeteringReportingService.REPORT_PERIOD,
                ServerlessSharedSettings.BOOST_WINDOW_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_SETTING
            )
        );
    }

    private static List<UsageRecord> pollRecords(BlockingQueue<UsageRecord> records, int num, TimeValue time) throws InterruptedException {
        List<UsageRecord> found = new ArrayList<>(num);
        long finishTime = System.nanoTime() + time.getNanos();
        do {
            long remaining = finishTime - System.nanoTime();
            UsageRecord r = records.poll(remaining, TimeUnit.NANOSECONDS);
            assertNotNull("Timed out waiting for record " + found.size(), r);
            found.add(r);
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

    public void testServiceReportsMetrics() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1));
        var counter2 = new TestCounter("counter2", Map.of("id", "counter2"), Map.of("setting", 2));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 3));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"), Map.of("setting", 4));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var now = new AtomicReference<>(Instant.EPOCH);
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> now.get());

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1, counter2),
                List.of(sampled1, sampled2),
                new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration)),
                REPORT_PERIOD,
                r -> addRecordsAndUpdateClock(r, records, now, reportPeriodDuration),
                threadPool,
                threadPool.generic(),
                clock
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            counter2.add(20);
            sampled1.set(50);
            sampled2.set(60);

            service.start();

            var reported = pollRecords(records, 4, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 15L, "counter2", 20L), Map.of("sampled1", 50L, "sampled2", 60L));

            service.stop();
        }
    }

    public void testCounterMetricsReset() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1));
        var counter2 = new TestCounter("counter2", Map.of("id", "counter2"), Map.of("setting", 2));

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1, counter2),
                List.of(),
                new InMemorySampledMetricsTimeCursor(),
                REPORT_PERIOD,
                records::addAll,
                threadPool,
                threadPool.generic()
            )
        ) {
            counter1.add(10);
            counter1.add(5);
            counter2.add(20);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 15L, "counter2", 20L), Map.of());

            // counters are reset after metrics are sent
            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 0L, "counter2", 0L), Map.of());

            counter1.add(5);
            counter2.add(10);

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 5L, "counter2", 10L), Map.of());

            service.stop();
        }
    }

    public void testSampledMetricsMaintained() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 1));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"), Map.of("setting", 2));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var now = new AtomicReference<>(Instant.EPOCH);
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> now.get());

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(),
                List.of(sampled1, sampled2),
                new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration)),
                REPORT_PERIOD,
                r -> addRecordsAndUpdateClock(r, records, now, reportPeriodDuration),
                threadPool,
                threadPool.generic(),
                clock
            )
        ) {
            sampled1.set(50);
            sampled2.set(60);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            sampled1.set(20);
            sampled2.set(65);

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 20L, "sampled2", 65L));

            service.stop();
        }
    }

    public void testMultipleRecordsSentUpToCommittedTimestamp() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 3));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var initialTime = Instant.EPOCH.minus(reportPeriodDuration.multipliedBy(2));
        var firstSampleTimestamp = initialTime.plus(reportPeriodDuration);
        var secondSampleTimestamp = firstSampleTimestamp.plus(reportPeriodDuration);
        var now = new AtomicReference<>(secondSampleTimestamp);
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> now.get());

        var cursor = new InMemorySampledMetricsTimeCursor(initialTime);

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                cursor,
                REPORT_PERIOD,
                r -> addRecordsAndUpdateClock(r, records, now, reportPeriodDuration),
                threadPool,
                threadPool.generic(),
                clock
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            sampled1.set(50);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 3, TimeValue.timeValueSeconds(10));

            var counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            var sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(2));

            assertThat(counterMetrics.get(0).usageTimestamp(), equalTo(secondSampleTimestamp));
            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(15L));

            assertThat(
                sampledMetrics.stream().map(UsageRecord::usageTimestamp).toList(),
                containsInAnyOrder(firstSampleTimestamp, secondSampleTimestamp)
            );

            assertThat(sampledMetrics.stream().map(usageRecord -> usageRecord.usage().quantity()).toList(), everyItem(is(50L)));
            service.stop();
        }
    }

    public void testRecordsNotSentAgainWhenTimestampCommitted() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 3));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        final var cursorTimestamp = new AtomicReference<>(Instant.EPOCH.minus(reportPeriodDuration));
        var firstSampleTimestamp = cursorTimestamp.get().plus(reportPeriodDuration);
        var secondSampleTimestamp = firstSampleTimestamp.plus(reportPeriodDuration);
        var now = new AtomicReference<>(firstSampleTimestamp);
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> now.get());

        var cursor = new SampledMetricsTimeCursor() {
            @Override
            public Instant getLatestCommitedTimestamp() {
                return cursorTimestamp.get();
            }

            @Override
            public void commitUpTo(Instant sampleTimestamp) {
                cursorTimestamp.set(sampleTimestamp);
            }
        };

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                cursor,
                REPORT_PERIOD,
                r -> addRecordsAndUpdateClock(r, records, now, reportPeriodDuration),
                threadPool,
                threadPool.generic(),
                clock
            )
        ) {

            counter1.add(10);
            counter1.add(5);
            sampled1.set(50);

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));

            var counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            var sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, hasSize(1));

            assertThat(counterMetrics.get(0).usageTimestamp(), equalTo(firstSampleTimestamp));
            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(15L));

            assertThat(sampledMetrics.get(0).usageTimestamp(), equalTo(firstSampleTimestamp));
            assertThat(sampledMetrics.get(0).usage().quantity(), equalTo(50L));

            cursor.commitUpTo(firstSampleTimestamp.plus(reportPeriodDuration.multipliedBy(2)));

            reported = pollRecords(records, 1, TimeValue.timeValueSeconds(10));

            assertThat(records, empty());
            counterMetrics = reported.stream().filter(x -> x.id().startsWith("counter1")).toList();
            sampledMetrics = reported.stream().filter(x -> x.id().startsWith("sampled1")).toList();
            assertThat(counterMetrics, hasSize(1));
            assertThat(sampledMetrics, empty());

            assertThat(counterMetrics.get(0).usageTimestamp(), equalTo(secondSampleTimestamp));
            assertThat(counterMetrics.get(0).usage().quantity(), equalTo(0L));

            service.stop();
        }
    }

    public void testSampledMetricsCalledConcurrentlyMatch() throws InterruptedException {

        final var results = new ConcurrentLinkedQueue<MetricValue>();

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);

        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 1));
        var sampled2 = new TestSample("sampled2", Map.of("id", "sampled2"), Map.of("setting", 2));

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
                x -> {},
                threadPool,
                threadPool.generic()
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
            new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1)),
            new TestCounter("counter2", Map.of("id", "counter2"), Map.of("setting", 2)) };

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
                x -> {},
                threadPool,
                threadPool.generic()
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

    public void testStopStops() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        var counter1 = new TestCounter("counter1", Map.of("id", "counter1"), Map.of("setting", 1));
        var sampled1 = new TestSample("sampled1", Map.of("id", "sampled1"), Map.of("setting", 2));

        var reportPeriodDuration = Duration.ofNanos(REPORT_PERIOD.getNanos());
        var now = new AtomicReference<>(Instant.EPOCH);
        var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(x -> now.get());

        try (
            MeteringReportingService service = new MeteringReportingService(
                NODE_ID,
                PROJECT_ID,
                List.of(counter1),
                List.of(sampled1),
                new InMemorySampledMetricsTimeCursor(now.get().minus(reportPeriodDuration)),
                REPORT_PERIOD,
                r -> addRecordsAndUpdateClock(r, records, now, reportPeriodDuration),
                threadPool,
                threadPool.generic(),
                clock
            )
        ) {
            counter1.add(15);
            sampled1.set(50);

            service.start();

            var reported = pollRecords(records, 2, TimeValue.timeValueSeconds(100));
            checkRecords(reported, Map.of("counter1", 15L), Map.of("sampled1", 50L));

            service.stop();

            counter1.add(20);
            sampled1.set(60);

            // no more records should appear
            assertNull(records.poll(6, TimeUnit.SECONDS));
        }
    }

    public void testServiceWithRealCollectors() throws IOException {
        ConcurrentLinkedQueue<UsageRecord> records = new ConcurrentLinkedQueue<>();
        var deterministicTaskQueue = new DeterministicTaskQueue();

        final int writerTasksCount = randomIntBetween(2, 6);
        final int writeOpsPerTask = randomIntBetween(50, 500);
        final int maxWriteIntervalInMillis = 1000;
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(maxWriteIntervalInMillis);

        final int numberOfIndexes = 1;
        final int numberOfShards = 5;
        final int docSize = 1;

        final long expectedIngestedDocumentSizeTotal = (long) writeOpsPerTask * writerTasksCount * docSize;
        final int estimatedMaxTimeInMillis = writeOpsPerTask * maxWriteIntervalInMillis;

        // Run for at least 2 REPORT_PERIOD to ensure metrics are collected for reporting at least once
        final long runningTimeInMillis = Math.max(REPORT_PERIOD.getMillis() * randomIntBetween(2, 6), estimatedMaxTimeInMillis);

        try (
            IndexSizeMetricsCollectorTests.TestIndex testIndex = IndexSizeMetricsCollectorTests.setUpIndicesService(
                "myIndex",
                numberOfIndexes,
                numberOfShards,
                2,
                3
            )
        ) {

            List<CounterMetricsCollector> builtInCounterMetrics = new ArrayList<>();
            List<SampledMetricsCollector> builtInSampledMetrics = new ArrayList<>();

            builtInSampledMetrics.add(new IndexSizeMetricsCollector(testIndex.indicesService(), clusterSettings, settings));
            var ingestMetricsCollector = new IngestMetricsCollector(NODE_ID, clusterSettings, settings);
            builtInCounterMetrics.add(ingestMetricsCollector);

            Consumer<Integer> writerAction = t -> {
                var indexId = t % numberOfIndexes;
                ingestMetricsCollector.addIngestedDocValue("myIndex" + indexId, docSize);
            };

            var threadPool = deterministicTaskQueue.getThreadPool();
            try (
                var service = new MeteringReportingService(
                    NODE_ID,
                    PROJECT_ID,
                    builtInCounterMetrics,
                    builtInSampledMetrics,
                    new InMemorySampledMetricsTimeCursor(),
                    REPORT_PERIOD,
                    records::addAll,
                    threadPool,
                    threadPool.generic()
                )
            ) {
                service.start();

                // Start a series of writer tasks
                for (int t = 0; t < writerTasksCount; t++) {
                    var threadId = t;
                    threadPool.generic().execute(new Runnable() {
                        private int opsCount = 0;

                        @Override
                        public void run() {
                            // To add additional randomness, let's perform operations in "batches", where multiple write operations
                            // happen without waiting.
                            final var candidateChunkSize = randomIntBetween(1, writeOpsPerTask / 10);
                            final var chunkSize = Math.min(candidateChunkSize, writeOpsPerTask - opsCount);

                            for (int i = 0; i < chunkSize; ++i) {
                                writerAction.accept(threadId);
                            }
                            opsCount += chunkSize;

                            var proceed = opsCount != writeOpsPerTask;
                            if (proceed == false) {
                                logger.info("writer [{}] finished", threadId);
                            } else {
                                threadPool.generic().execute(this);
                            }
                        }
                    });
                }

                // "Run" the tasks in the queue for the given amount of time
                while (deterministicTaskQueue.getCurrentTimeMillis() <= runningTimeInMillis) {
                    deterministicTaskQueue.runAllRunnableTasks();
                    deterministicTaskQueue.advanceTime();
                }

                service.stop();
            }
        }

        // The total size of ingested documents should be the same regardless of the order of execution/number of concurrent writes/number
        // of gatherings (number of usage records)
        assertThat(
            "Total size of ingested docs is correct",
            records.stream()
                .filter(usageRecord -> usageRecord.usage().type().equals("es_raw_data"))
                .mapToLong(x -> x.usage().quantity())
                .sum(),
            equalTo(expectedIngestedDocumentSizeTotal)
        );
        // We collected at least one index size record
        assertThat(
            "At least one index size record was collected",
            records.stream().filter(usageRecord -> usageRecord.usage().type().equals("es_indexed_data")).count(),
            is(greaterThan(0L))
        );
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static void addSampledResults(ConcurrentLinkedQueue<MetricValue> results, List<SampledMetricsCollector> collectors) {
        for (var collector : collectors) {
            var metrics = collector.getMetrics();
            assertThat(metrics, isPresent());
            metrics.get().forEach(results::add);
        }
    }

    private static void addCounterResults(ConcurrentLinkedQueue<MetricValue> results, List<CounterMetricsCollector> collectors) {
        for (var collector : collectors) {
            var metrics = collector.getMetrics();
            metrics.forEach(results::add);
            metrics.commit();
        }
    }

    private static void addRecordsAndUpdateClock(
        List<UsageRecord> report,
        BlockingQueue<UsageRecord> records,
        AtomicReference<Instant> now,
        Duration reportPeriodDuration
    ) {
        records.addAll(report);
        now.updateAndGet(x -> x.plus(reportPeriodDuration));
    }
}
