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

import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.MetricValue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterStateSupplier;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.Strings;
import org.elasticsearch.indices.SystemIndices;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for the ingest document size collection.
 * <p>
 * Accumulates metric values from ingestion.
 * <p>
 * Note on concurrency used here:
 * getMetrics is expected to run far fewer than addIngestedDocValue.
 * getMetrics by default should be triggered once every 5min
 * <p>
 * It is expected to have a lot (really a lot) of concurrent addIngestedDocValue calls, but most likely
 * on different index value.
 * <p>
 * ConcurrentHashMap - metrics - allows for safe concurrent updates on different indexNames
 * AtomicLong - a value of ConcurrentHashMap - allows for safe concurrent updates on the same indexName
 * <p>
 * We want to pause adding elements to a map when getMetrics is called. Otherwise, we risk a live lock when
 * getMetrics would be iterating over elements from ConcurrentHashMap and at the same time addIngestedDocValue would
 * be keep on adding more. Hence, getMetrics might never finish.
 * By using exclusiveLock (writeLock) we prevent any addIngestedDocValue when getMetrics is called.
 * By using nonExclusiveLock (readLock) we prevent getMetrics to be called at the same time as addIngestedDocValue
 * and we allow for concurrent addIngestedDocValue calls.
 */
public class IngestMetricsProvider implements CounterMetricsProvider {
    public static final String METRIC_TYPE = "es_raw_data";

    private final Logger logger = LogManager.getLogger(IngestMetricsProvider.class);
    private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReleasableLock exclusiveLock = new ReleasableLock(lock.writeLock());
    private final ReleasableLock nonExclusiveLock = new ReleasableLock(lock.readLock());
    private final String nodeId;
    private final ClusterStateSupplier clusterStateSupplier;
    private final SystemIndices systemIndices;

    public IngestMetricsProvider(String nodeId, ClusterStateSupplier clusterStateSupplier, SystemIndices systemIndices) {
        this.nodeId = nodeId;
        this.clusterStateSupplier = clusterStateSupplier;
        this.systemIndices = systemIndices;
    }

    private record SnapshotEntry(String key, long value) {}

    @Override
    public MetricValues getMetrics() {
        return clusterStateSupplier.withCurrentClusterState(clusterState -> {
            if (metrics.isEmpty()) {
                return CounterMetricsProvider.NO_VALUES;
            }

            final var metricsSnapshot = metrics.entrySet().stream().map(e -> new SnapshotEntry(e.getKey(), e.getValue().get())).toList();
            final var indicesLookup = clusterState.metadata().getIndicesLookup();

            final var toReturn = metricsSnapshot.stream()
                .map(e -> metricValue(nodeId, e.key(), e.value(), indicesLookup, systemIndices))
                .toList();
            logger.trace(() -> Strings.format("Metric values to be reported %s", toReturn));

            return new MetricValues() {
                @Override
                public Iterator<MetricValue> iterator() {
                    return toReturn.iterator();
                }

                @Override
                public void commit() {
                    adjustMap(metrics, metricsSnapshot);
                }
            };
        }, CounterMetricsProvider.NO_VALUES);
    }

    void adjustMap(Map<String, AtomicLong> metrics, List<SnapshotEntry> metricsSnapshot) {
        for (var snapshotEntry : metricsSnapshot) {
            AtomicLong value = metrics.get(snapshotEntry.key);
            assert (value != null);
            long newSize = value.addAndGet(-snapshotEntry.value());
            assert newSize >= 0;
            if (newSize == 0) {
                try (ReleasableLock ignored = exclusiveLock.acquire()) {
                    metrics.compute(snapshotEntry.key, (k, v) -> {
                        if (v != null && v.get() == 0) {
                            return null;
                        }
                        return v;
                    });
                }
            }
            logger.trace(() -> Strings.format("Adjusted counter for index [%s], newValue [%d]", snapshotEntry.key, newSize));
        }
    }

    public void addIngestedDocValue(String index, long size) {
        try (ReleasableLock ignored = nonExclusiveLock.acquire()) {
            AtomicLong currentValue = metrics.computeIfAbsent(index, (ind) -> new AtomicLong());
            long newSize = currentValue.addAndGet(size);

            logger.trace(() -> Strings.format("New ingested doc value %s for index %s, newValue %s", size, index, newSize));
        }
    }

    private static MetricValue metricValue(
        String nodeId,
        String index,
        long value,
        Map<String, IndexAbstraction> indicesLookup,
        SystemIndices systemIndices
    ) {
        // note: this is intentionally not resolved via IndexAbstraction, see https://elasticco.atlassian.net/browse/ES-10384
        final var isSystemIndex = systemIndices.isSystemIndex(index);
        final var indexAbstraction = indicesLookup.get(index);

        final var datastream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;
        Map<String, String> sourceMetadata = Maps.newHashMapWithExpectedSize(4);
        sourceMetadata.put(SourceMetadata.INDEX, index);
        sourceMetadata.put(SourceMetadata.SYSTEM_INDEX, Boolean.toString(isSystemIndex));
        if (indexAbstraction != null) {
            sourceMetadata.put(SourceMetadata.HIDDEN_INDEX, Boolean.toString(indexAbstraction.isHidden()));
        }
        if (datastream != null) {
            sourceMetadata.put(SourceMetadata.DATASTREAM, datastream.getName());
        }
        return new MetricValue("ingested-doc:" + index + ":" + nodeId, METRIC_TYPE, sourceMetadata, value, null);
    }
}
