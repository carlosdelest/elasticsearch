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
import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.apache.lucene.util.mutable.MutableValueLong;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

public class MeteringServiceTests extends ESTestCase {
    private static final String PROJECT_ID = "projectId";
    private static final String NODE_ID = "nodeId";

    private static final TimeValue REPORT_PERIOD = TimeValue.timeValueSeconds(5);

    private TestThreadPool threadPool;
    private Settings settings;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("meteringServiceTests");
        settings = Settings.builder()
            .put(MeteringService.PROJECT_ID.getKey(), PROJECT_ID)
            .put(MeteringService.REPORT_PERIOD.getKey(), REPORT_PERIOD)
            .build();
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
                    transformedMatch(r -> r.source().id(), equalTo(NODE_ID)),
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
                                transformedMatch(r -> r.usage().type(), equalTo(e.getKey())),
                                transformedMatch(r -> r.usage().quantity(), equalTo(e.getValue())),
                                transformedMatch(r -> r.source().metadata(), equalTo(Map.of("id", e.getKey())))
                            )
                        ),
                    sampledRecords.entrySet()
                        .stream()
                        .map(
                            e -> allOf(
                                transformedMatch(r -> r.usage().type(), equalTo(e.getKey())),
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

        try (MeteringService service = new MeteringService(NODE_ID, settings, records::addAll, threadPool)) {
            MetricsCollector.Counter counter1 = service.registerCounterMetric("counter1", Map.of("id", "counter1"));
            MetricsCollector.Counter counter2 = service.registerCounterMetric("counter2", Map.of("id", "counter2"));

            MutableValueLong sampled1 = new MutableValueLong();
            service.registerSampledMetric("sampled1", Map.of("id", "sampled1"), () -> sampled1.value);
            MutableValueLong sampled2 = new MutableValueLong();
            service.registerSampledMetric("sampled2", Map.of("id", "sampled2"), () -> sampled2.value);

            counter1.add(10);
            counter1.add(5);
            counter2.add(20);
            sampled1.value = 50;
            sampled2.value = 60;

            service.start();

            var reported = pollRecords(records, 4, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 15L, "counter2", 20L), Map.of("sampled1", 50L, "sampled2", 60L));

            service.stop();
        }
    }

    public void testCounterMetricsReset() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        try (MeteringService service = new MeteringService(NODE_ID, settings, records::addAll, threadPool)) {
            MetricsCollector.Counter counter1 = service.registerCounterMetric("counter1", Map.of("id", "counter1"));
            MetricsCollector.Counter counter2 = service.registerCounterMetric("counter2", Map.of("id", "counter2"));

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

        try (MeteringService service = new MeteringService(NODE_ID, settings, records::addAll, threadPool)) {
            MutableValueLong sampled1 = new MutableValueLong();
            service.registerSampledMetric("sampled1", Map.of("id", "sampled1"), () -> sampled1.value);
            MutableValueLong sampled2 = new MutableValueLong();
            service.registerSampledMetric("sampled2", Map.of("id", "sampled2"), () -> sampled2.value);

            sampled1.value = 50;
            sampled2.value = 60;

            service.start();

            List<UsageRecord> reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 50L, "sampled2", 60L));

            sampled1.value = 20;
            sampled2.value = 65;

            reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of(), Map.of("sampled1", 20L, "sampled2", 65L));

            service.stop();
        }
    }

    public void testStopStops() throws InterruptedException {
        BlockingQueue<UsageRecord> records = new LinkedBlockingQueue<>();

        try (MeteringService service = new MeteringService(NODE_ID, settings, records::addAll, threadPool)) {
            MetricsCollector.Counter counter1 = service.registerCounterMetric("counter1", Map.of("id", "counter1"));
            MutableValueLong sampled1 = new MutableValueLong();
            service.registerSampledMetric("sampled1", Map.of("id", "sampled1"), () -> sampled1.value);

            counter1.add(15);
            sampled1.value = 50;

            service.start();

            var reported = pollRecords(records, 2, TimeValue.timeValueSeconds(10));
            checkRecords(reported, Map.of("counter1", 15L), Map.of("sampled1", 50L));

            service.stop();

            counter1.add(20);
            sampled1.value = 60;

            // no more records should appear
            assertNull(records.poll(6, TimeUnit.SECONDS));
        }
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
