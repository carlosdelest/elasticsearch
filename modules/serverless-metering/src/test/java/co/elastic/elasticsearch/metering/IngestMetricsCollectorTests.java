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

import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.BOOST_WINDOW_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IngestMetricsCollectorTests extends ESTestCase {
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.builder().put(SEARCH_POWER_SETTING.getKey(), 100).put(BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(5)).build(),
        Set.of(SEARCH_POWER_SETTING, BOOST_WINDOW_SETTING)
    );

    public void testMetricIdUniqueness() {
        var ingestMetricsCollector1 = new IngestMetricsCollector("node1", clusterSettings, Settings.EMPTY);
        var ingestMetricsCollector2 = new IngestMetricsCollector("node2", clusterSettings, Settings.EMPTY);

        ingestMetricsCollector1.addIngestedDocValue("index", 10);

        ingestMetricsCollector2.addIngestedDocValue("index", 20);

        MetricsCollector.MetricValue first = ingestMetricsCollector1.getMetrics().stream().findFirst().get();
        MetricsCollector.MetricValue second = ingestMetricsCollector2.getMetrics().stream().findFirst().get();

        assertThat(first.id(), equalTo("ingested-doc:index:node1"));
        assertThat(second.id(), equalTo("ingested-doc:index:node2"));
    }

    public void testConcurrencyManyWritersOneReaderNoWait() throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();
        final var ingestMetricsCollector = new IngestMetricsCollector("node", clusterSettings, Settings.EMPTY);

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = 1;
        final int docSize = randomIntBetween(1, 10);

        final long totalOps = (long) writeOpsPerThread * writerThreadsCount;

        performConcurrencyTest(
            writerThreadsCount,
            writeOpsPerThread,
            () -> 0,
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> 0,
            () -> results.addAll(ingestMetricsCollector.getMetrics())
        );

        long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();
        var itemsLeft = ingestMetricsCollector.getMetrics().size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }

    public void testConcurrencyManyWritersManyReadersNoWait() throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();
        final var ingestMetricsCollector = new IngestMetricsCollector("node", clusterSettings, Settings.EMPTY);

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = randomIntBetween(2, 4);
        final int docSize = randomIntBetween(1, 10);

        final long totalOps = (long) writeOpsPerThread * writerThreadsCount;

        performConcurrencyTest(
            writerThreadsCount,
            writeOpsPerThread,
            () -> 0,
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> 0,
            () -> results.addAll(ingestMetricsCollector.getMetrics())
        );

        long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();
        var itemsLeft = ingestMetricsCollector.getMetrics().size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }

    public void testConcurrencyManyWritersOneReaderWithWait() throws InterruptedException {

        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();
        final var ingestMetricsCollector = new IngestMetricsCollector("node", clusterSettings, Settings.EMPTY);

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = 1;
        final int docSize = randomIntBetween(1, 10);

        final long totalOps = (long) writeOpsPerThread * writerThreadsCount;

        performConcurrencyTest(
            writerThreadsCount,
            writeOpsPerThread,
            () -> randomIntBetween(10, 50),
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> randomIntBetween(100, 200),
            () -> results.addAll(ingestMetricsCollector.getMetrics())
        );

        long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();
        var itemsLeft = ingestMetricsCollector.getMetrics().size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }

    /**
     * Performs a predefined number of write operations using different parameters (number of writers, delay between writes, etc.) and
     * a complimentary number of collect operations (sufficient to collect all writes - again using different parameters)
     * Write and collect actions are performed in a concurrent way using real threads to simulate more accurately access patterns,
     * preemption and interaction with real concurrent data structures and primitives.
     */
    private void performConcurrencyTest(
        int writerThreadsCount,
        int writeOpsPerThread,
        Supplier<Integer> writeIntervalInMillis,
        Consumer<Integer> writerAction,
        int collectThreadsCount,
        Supplier<Integer> collectIntervalInMillis,
        Runnable collectorAction
    ) throws InterruptedException {

        final Thread[] writerThreads = new Thread[writerThreadsCount];

        final int totalOps = writeOpsPerThread * writerThreads.length;

        logger.info("--> will run [{}] threads, totalOps[{}]", writerThreads.length, totalOps);
        final CyclicBarrier barrier = new CyclicBarrier(writerThreadsCount + collectThreadsCount);
        for (int t = 0; t < writerThreads.length; t++) {
            var threadId = t;
            writerThreads[t] = createWriterThread(
                threadId,
                barrier,
                writeOpsPerThread,
                () -> writerAction.accept(threadId),
                writeIntervalInMillis.get()
            );
            writerThreads[t].start();
        }

        AtomicBoolean writeFinished = new AtomicBoolean(false);
        final Thread[] collectThreads = new Thread[collectThreadsCount];

        for (int t = 0; t < collectThreads.length; t++) {
            collectThreads[t] = createCollectorThread(t, barrier, writeFinished, collectorAction, collectIntervalInMillis.get());
            collectThreads[t].start();
        }

        for (Thread thread : writerThreads) {
            thread.join();
        }
        writeFinished.set(true);
        for (Thread thread : collectThreads) {
            thread.join();
        }
    }

    /**
     * Creates a collector thread; this thread will perform a provided {@code collectAction} every {@code collectIntervalInMillis}.
     */
    private Thread createCollectorThread(
        int threadId,
        CyclicBarrier barrier,
        AtomicBoolean writeFinished,
        Runnable collectAction,
        int collectIntervalInMillis
    ) {
        return new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("failure in background thread", e);
            }

            @SuppressWarnings("BusyWait")
            @Override
            protected void doRun() throws Exception {
                barrier.await();
                logger.info("collector [{}] started", threadId);
                while (writeFinished.get() == false) {
                    // logger.info("reading");
                    collectAction.run();

                    // To add some randomness, make it possible for a thread to "skip" the wait.
                    // If we passed 0 to collectIntervalInMillis, not skipping will still have an effect
                    // (there will be no wait but a context switch will likely happen)
                    if (frequently()) {
                        Thread.sleep(collectIntervalInMillis);
                    }
                }
                // "flush" any pending data yet to collect
                collectAction.run();
                logger.info("collector [{}] finished", threadId);
            }
        }, "testIngestMetricsCollectorCollector_" + threadId);
    }

    /**
     * Creates a collector thread; this thread will perform a provided {@code writeAction} every {@code writeIntervalInMillis}.
     */
    private Thread createWriterThread(
        int threadId,
        CyclicBarrier barrier,
        int opsPerThread,
        Runnable writeAction,
        int writeIntervalInMillis
    ) {
        return new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("failure in background thread", e);
            }

            @Override
            protected void doRun() throws Exception {
                barrier.await();
                logger.info("writer [{}] started", threadId);

                var opsCount = 0;
                while (opsCount != opsPerThread) {
                    // To add additional randomness, let's perform operations in "batches", where multiple write operations
                    // happen without waiting.
                    final var candidateChunkSize = randomIntBetween(1, opsPerThread / 10);
                    final var chunkSize = Math.min(candidateChunkSize, opsPerThread - opsCount);
                    for (int i = 0; i < chunkSize; ++i) {
                        writeAction.run();
                    }
                    opsCount += chunkSize;
                    // Note: if we passed 0 to writeIntervalInMillis, this will still have an effect
                    // (there will be no wait but a context switch will likely happen)
                    Thread.sleep(writeIntervalInMillis);
                }
                logger.info("writer [{}] finished", threadId);
            }
        }, "testIngestMetricsCollectorWriter_" + threadId);
    }
}
