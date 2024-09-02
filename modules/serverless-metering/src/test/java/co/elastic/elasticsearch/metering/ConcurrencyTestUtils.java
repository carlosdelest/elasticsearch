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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.frequently;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

class ConcurrencyTestUtils {
    /**
     * Performs a predefined number of operations using different parameters (number of threads, delay between steps, etc.) paired with
     * a number of complementary concurrent "collector" actions (sufficient to complement all operations - again using different parameters)
     * Operations and collectors are performed in a concurrent way using real threads to simulate more accurately access patterns,
     * preemption and interaction with real concurrent data structures and primitives.
     */
    static void runConcurrentWithCollectors(
        int threadsCount,
        int opsPerThread,
        Supplier<Integer> intervalInMillis,
        Consumer<Integer> action,
        int collectThreadsCount,
        Supplier<Integer> collectIntervalInMillis,
        Runnable collectorAction,
        Consumer<String> output
    ) throws InterruptedException {

        final Thread[] operationThreads = new Thread[threadsCount];

        final int totalOps = opsPerThread * operationThreads.length;

        output.accept(String.format(Locale.ROOT, "--> will run [%d] threads, totalOps[%d]", operationThreads.length, totalOps));

        // Create a barrier for all threads (main operation + result collection) to sync on - start executing their respective actions
        // at the same time
        final CyclicBarrier barrier = new CyclicBarrier(threadsCount + collectThreadsCount);
        for (int t = 0; t < operationThreads.length; t++) {
            operationThreads[t] = createActionThread(
                t,
                opsPerThread,
                barrier,
                () -> action.accept(randomIntBetween(0, operationThreads.length - 1)),
                intervalInMillis.get(),
                output
            );
            operationThreads[t].start();
        }

        AtomicBoolean operationsFinished = new AtomicBoolean(false);
        final Thread[] collectThreads = new Thread[collectThreadsCount];

        for (int t = 0; t < collectThreads.length; t++) {
            collectThreads[t] = createCollectorThread(
                t,
                barrier,
                operationsFinished,
                collectorAction,
                collectIntervalInMillis.get(),
                output
            );
            collectThreads[t].start();
        }

        for (Thread thread : operationThreads) {
            thread.join();
        }
        operationsFinished.set(true);
        for (Thread thread : collectThreads) {
            thread.join();
        }
    }

    /**
     * Creates a collector thread; this thread will perform a provided {@code collectAction} every {@code collectIntervalInMillis}.
     */
    private static Thread createCollectorThread(
        int threadId,
        CyclicBarrier barrier,
        AtomicBoolean writeFinished,
        Runnable collectAction,
        int collectIntervalInMillis,
        Consumer<String> output
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
                output.accept(String.format(Locale.ROOT, "Collector thread [%d] started", threadId));
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
                output.accept(String.format(Locale.ROOT, "Collector thread [%d] finished", threadId));
            }
        }, "testMetricsCollector_" + threadId);
    }

    /**
     * Creates an action thread; this thread will perform a provided {@code action} every {@code intervalInMillis}.
     */
    private static Thread createActionThread(
        int threadId,
        int opsPerThread,
        CyclicBarrier barrier,
        Runnable action,
        int intervalInMillis,
        Consumer<String> output
    ) {
        return new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("failure in background thread", e);
            }

            @Override
            protected void doRun() throws Exception {
                barrier.await();
                output.accept(String.format(Locale.ROOT, "Action thread [%d] started", threadId));

                var opsCount = 0;
                while (opsCount != opsPerThread) {
                    // To add additional randomness, let's perform operations in "batches", where multiple write operations
                    // happen without waiting.
                    final var candidateChunkSize = randomIntBetween(1, opsPerThread / 10);
                    final var chunkSize = Math.min(candidateChunkSize, opsPerThread - opsCount);
                    for (int i = 0; i < chunkSize; ++i) {
                        action.run();
                    }
                    opsCount += chunkSize;
                    // Note: if we passed 0 to intervalInMillis, this will still have an effect
                    // (there will be no wait but a context switch will likely happen)
                    Thread.sleep(intervalInMillis);
                }
                output.accept(String.format(Locale.ROOT, "Action thread [%d] finished", threadId));
            }
        }, "testMetricsActor_" + threadId);
    }
}
