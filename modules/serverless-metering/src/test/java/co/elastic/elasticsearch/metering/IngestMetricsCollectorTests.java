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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.BOOST_WINDOW_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IngestMetricsCollectorTests extends ESTestCase {
    private final int spMin = 100;
    private final int spMax = 200;

    private final Settings settings = Settings.builder()
        .put(SEARCH_POWER_MIN_SETTING.getKey(), spMin)
        .put(SEARCH_POWER_MAX_SETTING.getKey(), spMax)
        .put(BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(5))
        .build();
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        settings,
        Set.of(SEARCH_POWER_MIN_SETTING, SEARCH_POWER_MAX_SETTING, BOOST_WINDOW_SETTING, SEARCH_POWER_SETTING)
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

        ConcurrencyTestUtils.runConcurrentWithCollectors(
            writerThreadsCount,
            writeOpsPerThread,
            () -> 0,
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> 0,
            () -> results.addAll(ingestMetricsCollector.getMetrics()),
            logger::info
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

        ConcurrencyTestUtils.runConcurrentWithCollectors(
            writerThreadsCount,
            writeOpsPerThread,
            () -> 0,
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> 0,
            () -> results.addAll(ingestMetricsCollector.getMetrics()),
            logger::info
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

        ConcurrencyTestUtils.runConcurrentWithCollectors(
            writerThreadsCount,
            writeOpsPerThread,
            () -> randomIntBetween(10, 50),
            t -> ingestMetricsCollector.addIngestedDocValue("index" + t, docSize),
            collectThreadsCount,
            () -> randomIntBetween(100, 200),
            () -> results.addAll(ingestMetricsCollector.getMetrics()),
            logger::info
        );

        long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();
        var itemsLeft = ingestMetricsCollector.getMetrics().size();
        assertThat(valueSum, equalTo(totalOps * docSize));
        assertThat(itemsLeft, is(0));
    }
}
