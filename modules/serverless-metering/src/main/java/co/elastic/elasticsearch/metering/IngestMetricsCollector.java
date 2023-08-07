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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.Strings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Responsible for the ingest document size collection.
 * <p>
 * Accumulates metric values from ingestion.
 *
 * Note on concurrency used here:
 * getMetrics is expected to run far fewer than addIngestedDocValue.
 * getMetrics by default should be triggered once every 5min
 *
 * It is expected to have a lot (really a lot) of concurrent addIngestedDocValue calls, but most likely
 * on different index value.
 *
 * ConcurrentHashMap - metrics - allows for safe concurrent updates on different indexNames
 * AtomicLong - a value of ConcurrentHashMap - allows for safe concurrent updates on the same indexName
 *
 * We want to pause adding elements to a map when getMetrics is called. Otherwise, we risk a live lock when
 * getMetrics would be iterating over elements from ConcurrentHashMap and at the same time addIngestedDocValue would
 * be keep on adding more. Hence, getMetrics might never finish.
 * By using exclusiveLock (writeLock) we prevent any addIngestedDocValue when getMetrics is called.
 * By using nonExclusiveLock (readLock) we prevent getMetrics to be called at the same time as addIngestedDocValue
 * and we allow for concurrent addIngestedDocValue calls.
 */
public class IngestMetricsCollector implements MetricsCollector {
    public static final String METRIC_TYPE = "es_raw_data";
    private final Logger logger = LogManager.getLogger(IngestMetricsCollector.class);
    private Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReleasableLock exclusiveLock = new ReleasableLock(lock.writeLock());
    private final ReleasableLock nonExclusiveLock = new ReleasableLock(lock.readLock());
    private final String nodeId;

    public IngestMetricsCollector(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public Collection<MetricValue> getMetrics() {
        Map<String, AtomicLong> oldMetrics;
        Map<String, AtomicLong> emptyMetrics = new ConcurrentHashMap<>();
        try (ReleasableLock ignored = exclusiveLock.acquire()) {
            oldMetrics = metrics;
            metrics = emptyMetrics;
        }
        List<MetricValue> toReturn = oldMetrics.entrySet()
            .stream()
            .map(e -> metricValue(e.getKey(), e.getValue().longValue()))
            .collect(Collectors.toList());

        logger.trace(() -> Strings.format("Metric values to be reported %s", toReturn));
        return toReturn;

    }

    public void addIngestedDocValue(String index, long size) {
        try (ReleasableLock ignored = nonExclusiveLock.acquire()) {
            AtomicLong currentValue = metrics.computeIfAbsent(index, (ind) -> new AtomicLong());
            long newSize = currentValue.addAndGet(size);

            logger.trace(() -> Strings.format("New ingested doc value %s for index %s, newValue %s", size, index, newSize));
        }
    }

    private MetricValue metricValue(String index, long value) {
        return new MetricValue(MeasurementType.COUNTER, "ingested-doc:" + index + ":" + nodeId, METRIC_TYPE, Map.of("index", index), value);
    }
}
