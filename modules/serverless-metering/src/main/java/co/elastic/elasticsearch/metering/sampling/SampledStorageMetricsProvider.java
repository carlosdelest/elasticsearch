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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

class SampledStorageMetricsProvider implements SampledMetricsProvider {
    private static final String IX_METRIC_TYPE = "es_indexed_data";
    static final String IX_METRIC_ID_PREFIX = "shard-size";
    private static final String RA_S_METRIC_TYPE = "es_raw_stored_data";
    static final String RA_S_METRIC_ID_PREFIX = "raw-stored-index-size";
    private static final String METADATA_PARTIAL_KEY = "partial";
    private static final String METADATA_INDEX_KEY = "index";
    private static final String METADATA_SHARD_KEY = "shard";
    private static final String METADATA_DATASTREAM_KEY = "datastream";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final ClusterService clusterService;

    SampledStorageMetricsProvider(SampledClusterMetricsService sampledClusterMetricsService, ClusterService clusterService) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.clusterService = clusterService;
    }

    @Override
    public Optional<MetricValues> getMetrics() {
        return sampledClusterMetricsService.withSamplesIfReady(this::sampleToMetricValues);
    }

    private MetricValues sampleToMetricValues(SampledClusterMetricsService.SampledClusterMetrics sample) {
        final var clusterStateMetadata = clusterService.state().getMetadata();
        boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
        List<MetricValue> metrics = new ArrayList<>();
        for (final var shardEntry : sample.shardSamples().entrySet()) {
            long size = shardEntry.getValue().shardInfo().totalSizeInBytes();
            // Do not generate records with size 0
            if (size > 0) {
                int shardId = shardEntry.getKey().shardId();
                var indexName = shardEntry.getKey().indexName();
                var indexCreationDate = Instant.ofEpochMilli(shardEntry.getValue().shardInfo().indexCreationDateEpochMilli());

                Map<String, String> sourceMetadata = new HashMap<>();
                sourceMetadata.put(METADATA_SHARD_KEY, Integer.toString(shardId));
                fillIndexMetadata(sourceMetadata, clusterStateMetadata, indexName, partial);
                metrics.add(
                    new MetricValue(
                        format("%s:%s", IX_METRIC_ID_PREFIX, shardEntry.getKey()),
                        IX_METRIC_TYPE,
                        sourceMetadata,
                        size,
                        indexCreationDate
                    )
                );
            }
        }

        Map<String, RaStorageInfo> raStorageInfos = sample.shardSamples()
            .entrySet()
            .stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getKey().indexName(),
                    Collector.of(RaStorageInfo::new, RaStorageInfo::accumulate, RaStorageInfo::combine)
                )
            );
        for (final var indexEntry : raStorageInfos.entrySet()) {
            final var storedIngestSizeInBytes = indexEntry.getValue().raStorageSize;
            if (storedIngestSizeInBytes > 0) {
                final var indexName = indexEntry.getKey();
                final var indexCreationDate = indexEntry.getValue().indexCreationDate;

                Map<String, String> metadata = new HashMap<>();
                fillIndexMetadata(metadata, clusterStateMetadata, indexName, partial);
                metrics.add(
                    new MetricValue(
                        format("%s:%s", RA_S_METRIC_ID_PREFIX, indexName),
                        RA_S_METRIC_TYPE,
                        metadata,
                        storedIngestSizeInBytes,
                        indexCreationDate
                    )
                );
            }
        }
        return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
    }

    private void fillIndexMetadata(Map<String, String> sourceMetadata, Metadata clusterMetadata, String indexName, boolean partial) {
        final var indexAbstraction = clusterMetadata.getIndicesLookup().get(indexName);
        final boolean inDatastream = indexAbstraction != null && indexAbstraction.getParentDataStream() != null;

        sourceMetadata.put(METADATA_INDEX_KEY, indexName);
        if (partial) {
            sourceMetadata.put(METADATA_PARTIAL_KEY, Boolean.TRUE.toString());
        }
        if (inDatastream) {
            sourceMetadata.put(METADATA_DATASTREAM_KEY, indexAbstraction.getParentDataStream().getName());
        }
    }

    private static final class RaStorageInfo {
        private Instant indexCreationDate;
        private long raStorageSize;

        void accumulate(Map.Entry<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> t) {
            raStorageSize += t.getValue().shardInfo().rawStoredSizeInBytes();
            indexCreationDate = getEarlierValidCreationDate(
                indexCreationDate,
                Instant.ofEpochMilli(t.getValue().shardInfo().indexCreationDateEpochMilli())
            );
        }

        RaStorageInfo combine(RaStorageInfo b) {
            raStorageSize += b.raStorageSize;
            indexCreationDate = getEarlierValidCreationDate(indexCreationDate, b.indexCreationDate);
            return this;
        }

        private static Instant getEarlierValidCreationDate(Instant a, Instant b) {
            if (a == null || (b != null && b.isBefore(a))) {
                return b;
            }
            return a;
        }
    }
}
