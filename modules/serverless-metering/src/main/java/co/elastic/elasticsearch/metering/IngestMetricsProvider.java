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
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * on different indices.
 * <p>
 * ConcurrentHashMap - ingestMeters - allows for safe concurrent updates on different indices.
 * Modifications to ingest meters (both for increments and decrements - when committing) are done via compute which is atomic.
 * Reading of snapshots requires volatile reads of IngestMeter#bytes.
 */
public class IngestMetricsProvider implements CounterMetricsProvider {
    public static final String METRIC_TYPE = "es_raw_data";

    private static final Logger logger = LogManager.getLogger(IngestMetricsProvider.class);

    private final Map<Index, IngestMeter> ingestMeters = new ConcurrentHashMap<>();
    private final String nodeId;
    private final ClusterStateSupplier clusterStateSupplier;
    private final SystemIndices systemIndices;

    public IngestMetricsProvider(String nodeId, ClusterStateSupplier clusterStateSupplier, SystemIndices systemIndices) {
        this.nodeId = nodeId;
        this.clusterStateSupplier = clusterStateSupplier;
        this.systemIndices = systemIndices;
    }

    private record IngestSnapshot(Index key, long bytes, Map<String, String> sourceMetadata) {
        IngestSnapshot(Map.Entry<Index, IngestMeter> entry) {
            this(entry.getKey(), entry.getValue().bytes, entry.getValue().sourceMetadata);
        }
    }

    private class IngestMeter {
        private final Map<String, String> sourceMetadata;
        private volatile long bytes; // volatile read necessary to create snapshots

        IngestMeter(long bytes, Map<String, String> sourceMetadata) {
            this.sourceMetadata = sourceMetadata;
            this.bytes = bytes;
        }
    }

    @Override
    public MetricValues getMetrics() {
        if (ingestMeters.isEmpty()) {
            return CounterMetricsProvider.NO_VALUES;
        }

        final var metricSnapshots = ingestMeters.entrySet().stream().map(IngestSnapshot::new).toList();
        final var metricValues = metricSnapshots.stream().filter(s -> s.bytes > 0).map(this::metricValue).toList();
        logger.trace(() -> Strings.format("Metric values to be reported %s", metricValues));

        return new MetricValues() {
            @Override
            public Iterator<MetricValue> iterator() {
                return metricValues.iterator();
            }

            @Override
            public void commit() {
                adjustMeters(metricSnapshots);
            }
        };
    }

    private void adjustMeters(List<IngestSnapshot> metricsSnapshot) {
        for (var snapshotEntry : metricsSnapshot) {
            ingestMeters.compute(snapshotEntry.key, (index, meter) -> {
                if (snapshotEntry.bytes == 0) {
                    // consider index to be stale (and evict) if still 0
                    return meter == null || meter.bytes == 0 ? null : meter;
                } else {
                    assert meter != null;
                    var newSize = meter.bytes - snapshotEntry.bytes;
                    assert newSize >= 0;
                    meter.bytes = newSize;
                    return meter;
                }
            });

        }
    }

    public void addIngestedDocValue(Index index, long size) {
        ingestMeters.compute(index, (idx, meter) -> {
            if (meter == null) {
                return initIngestMeter(idx, size);
            }
            meter.bytes += size;
            return meter;
        });
    }

    private IngestMeter initIngestMeter(Index index, long size) {
        var lookup = clusterStateSupplier.withCurrentClusterState(s -> s.getMetadata().getDefaultProject().getIndicesLookup(), null);
        assert lookup != null;
        var indexAbstraction = lookup != null ? lookup.get(index.getName()) : null;
        return new IngestMeter(size, SourceMetadata.indexSourceMetadata(index, indexAbstraction, systemIndices));
    }

    private MetricValue metricValue(IngestSnapshot snapshot) {
        return new MetricValue(
            "ingested-doc:" + snapshot.key.getUUID() + ":" + nodeId,
            METRIC_TYPE,
            snapshot.sourceMetadata,
            snapshot.bytes,
            null
        );
    }
}
