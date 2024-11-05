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

import co.elastic.elasticsearch.metrics.MetricValue;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static co.elastic.elasticsearch.metering.TestUtils.iterableToList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IngestMetricsProviderTests extends ESTestCase {

    private final ClusterStateSupplier clusterStateSupplier = () -> Optional.of(ClusterState.EMPTY_STATE);

    public void testMetricIdUniqueness() {
        var ingestMetricsProvider1 = new IngestMetricsProvider("node1", clusterStateSupplier);
        var ingestMetricsProvider2 = new IngestMetricsProvider("node2", clusterStateSupplier);

        ingestMetricsProvider1.addIngestedDocValue("index", 10);

        ingestMetricsProvider2.addIngestedDocValue("index", 20);

        var first = ingestMetricsProvider1.getMetrics().iterator().next();
        var second = ingestMetricsProvider2.getMetrics().iterator().next();

        assertThat(first.id(), equalTo("ingested-doc:index:node1"));
        assertThat(second.id(), equalTo("ingested-doc:index:node2"));
    }

    public void testMetricsValueRemainsIfNotCommited() {
        final var ingestMetricsProvider = new IngestMetricsProvider("node", clusterStateSupplier);
        final int docSize = randomIntBetween(1, 10);

        ingestMetricsProvider.addIngestedDocValue("index1", docSize);
        ingestMetricsProvider.addIngestedDocValue("index2", docSize);
        ingestMetricsProvider.addIngestedDocValue("index3", docSize);

        var metrics = iterableToList(ingestMetricsProvider.getMetrics());
        long valueSum = metrics.stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(3L * docSize));

        metrics = iterableToList(ingestMetricsProvider.getMetrics());
        valueSum = metrics.stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(3L * docSize));
    }

    public void testMetricsValueKeepsCountingUntilCommited() {
        final var ingestMetricsProvider = new IngestMetricsProvider("node", clusterStateSupplier);
        final int docSize = randomIntBetween(1, 10);

        ingestMetricsProvider.addIngestedDocValue("index1", docSize);
        ingestMetricsProvider.addIngestedDocValue("index2", docSize);
        ingestMetricsProvider.addIngestedDocValue("index3", docSize);

        var metrics = ingestMetricsProvider.getMetrics();
        long valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(3L * docSize));

        ingestMetricsProvider.addIngestedDocValue("index3", docSize);

        metrics = ingestMetricsProvider.getMetrics();
        valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(4L * docSize));

        metrics.commit();

        metrics = ingestMetricsProvider.getMetrics();
        valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(0L));
    }

    public void testMetricsValueRestartCountingAfterCommited() {
        final var ingestMetricsProvider = new IngestMetricsProvider("node", clusterStateSupplier);
        final long docSize = randomIntBetween(1, 10);

        ingestMetricsProvider.addIngestedDocValue("index1", docSize);
        ingestMetricsProvider.addIngestedDocValue("index2", docSize);
        ingestMetricsProvider.addIngestedDocValue("index3", docSize);

        var metrics = ingestMetricsProvider.getMetrics();
        long valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(3L * docSize));
        metrics.commit();

        final long docSize2 = randomIntBetween(1, 10);
        ingestMetricsProvider.addIngestedDocValue("index3", docSize2);

        metrics = ingestMetricsProvider.getMetrics();
        valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(docSize2));

        ingestMetricsProvider.addIngestedDocValue("index1", docSize2);
        ingestMetricsProvider.addIngestedDocValue("index3", docSize2);

        metrics = ingestMetricsProvider.getMetrics();
        valueSum = iterableToList(metrics).stream().mapToLong(MetricValue::value).sum();

        assertThat(valueSum, equalTo(3L * docSize2));
    }

    public void testConcurrencyManyWritersOneReaderNoWait() throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<MetricValue>();
        final var ingestMetricsProvider = new IngestMetricsProvider("node", clusterStateSupplier);

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = 1;
        final int docSize = randomIntBetween(1, 10);

        final long totalOps = (long) writeOpsPerThread * writerThreadsCount;

        ConcurrencyTestUtils.runConcurrentWithCollectors(
            writerThreadsCount,
            writeOpsPerThread,
            () -> 0,
            t -> ingestMetricsProvider.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> 0,
            () -> {
                var metrics = ingestMetricsProvider.getMetrics();
                metrics.forEach(results::add);
                metrics.commit();
            },
            logger::info
        );

        long valueSum = results.stream().mapToLong(MetricValue::value).sum();
        var itemsLeft = iterableToList(ingestMetricsProvider.getMetrics()).size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }

    public void testConcurrencyManyWritersOneReaderWithWait() throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<MetricValue>();
        final var ingestMetricsProvider = new IngestMetricsProvider("node", clusterStateSupplier);

        final int writerThreadsCount = randomIntBetween(4, 10);
        final int writeOpsPerThread = randomIntBetween(100, 2000);
        final int collectThreadsCount = 1;
        final int docSize = randomIntBetween(1, 10);

        final long totalOps = (long) writeOpsPerThread * writerThreadsCount;

        ConcurrencyTestUtils.runConcurrentWithCollectors(
            writerThreadsCount,
            writeOpsPerThread,
            () -> randomIntBetween(10, 50),
            t -> ingestMetricsProvider.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> randomIntBetween(100, 200),
            () -> {
                var metrics = ingestMetricsProvider.getMetrics();
                metrics.forEach(results::add);
                metrics.commit();
            },
            logger::info
        );

        long valueSum = results.stream().mapToLong(MetricValue::value).sum();
        var itemsLeft = iterableToList(ingestMetricsProvider.getMetrics()).size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }
}
