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

import co.elastic.elasticsearch.metering.SourceMetadata;
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.SystemIndices;

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

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;

    SampledStorageMetricsProvider(
        SampledClusterMetricsService sampledClusterMetricsService,
        ClusterService clusterService,
        SystemIndices systemIndices
    ) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
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
            ShardInfoMetrics shardMetrics = shardEntry.getValue().shardInfo();
            long size = shardMetrics.totalSizeInBytes();
            // Do not generate records with size 0
            if (size > 0) {
                int shardId = shardEntry.getKey().shardId();
                var indexName = shardEntry.getKey().indexName();
                var indexCreationDate = Instant.ofEpochMilli(shardMetrics.indexCreationDateEpochMilli());

                Map<String, String> sourceMetadata = new HashMap<>();
                sourceMetadata.put(SourceMetadata.SHARD, Integer.toString(shardId));
                fillIndexMetadata(sourceMetadata, clusterStateMetadata, indexName, partial);

                Map<String, String> usageMetadata = new HashMap<>();
                fillIXUsageMetadata(usageMetadata, shardMetrics);
                metrics.add(
                    new MetricValue(
                        format("%s:%s", IX_METRIC_ID_PREFIX, shardEntry.getKey()),
                        IX_METRIC_TYPE,
                        sourceMetadata,
                        usageMetadata,
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

                Map<String, String> sourceMetadata = new HashMap<>();
                Map<String, String> usageMetadata = new HashMap<>();
                fillIndexMetadata(sourceMetadata, clusterStateMetadata, indexName, partial);
                fillRAStorageMetadata(usageMetadata, indexEntry.getValue());
                metrics.add(
                    new MetricValue(
                        format("%s:%s", RA_S_METRIC_ID_PREFIX, indexName),
                        RA_S_METRIC_TYPE,
                        sourceMetadata,
                        usageMetadata,
                        storedIngestSizeInBytes,
                        indexCreationDate
                    )
                );
            }
        }
        return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
    }

    private void fillIndexMetadata(Map<String, String> sourceMetadata, Metadata clusterMetadata, String indexName, boolean partial) {
        // note: this is intentionally not resolved via IndexAbstraction, see https://elasticco.atlassian.net/browse/ES-10384
        final var isSystemIndex = systemIndices.isSystemIndex(indexName);
        final var indexAbstraction = clusterMetadata.getProject().getIndicesLookup().get(indexName);
        final var datastream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;

        sourceMetadata.put(SourceMetadata.INDEX, indexName);
        sourceMetadata.put(SourceMetadata.SYSTEM_INDEX, Boolean.toString(isSystemIndex));
        if (indexAbstraction != null) {
            sourceMetadata.put(SourceMetadata.HIDDEN_INDEX, Boolean.toString(indexAbstraction.isHidden()));
        }
        if (partial) {
            sourceMetadata.put(METADATA_PARTIAL_KEY, Boolean.TRUE.toString());
        }
        if (datastream != null) {
            sourceMetadata.put(SourceMetadata.DATASTREAM, datastream.getName());
        }
    }

    private void fillIXUsageMetadata(Map<String, String> usageMetadata, ShardInfoMetrics shardMetrics) {
        usageMetadata.put("segment_count", Long.toString(shardMetrics.segmentCount()));
        usageMetadata.put("doc_count", Long.toString(shardMetrics.docCount()));
        usageMetadata.put("deleted_doc_count", Long.toString(shardMetrics.deletedDocCount()));
        usageMetadata.put("interactive_size", Long.toString(shardMetrics.interactiveSizeInBytes()));
    }

    private void fillRAStorageMetadata(Map<String, String> usageMetadata, RaStorageInfo info) {
        usageMetadata.put("segment_count", Long.toString(info.segmentCount));
        usageMetadata.put("doc_count", Long.toString(info.liveDocCount));
        usageMetadata.put("deleted_doc_count", Long.toString(info.deletedDocCount));

        if (info.hasRAStats) {
            usageMetadata.put("ra_size_segment_count", Long.toString(info.raSegmentCount));
            usageMetadata.put("ra_size_doc_count", Long.toString(info.raLiveDocCount));
            usageMetadata.put("ra_size_deleted_doc_count", Long.toString(info.raDeletedDocCount));
            usageMetadata.put("ra_size_approximated_doc_count", Long.toString(info.raApproximatedDocCount));

            long avg = (long) (info.raAvgTotal / info.raSegmentCount);
            long stddev = (long) Math.sqrt(info.raAvgSquaredTotal / info.raSegmentCount - Math.pow(avg, 2));
            usageMetadata.put("ra_size_segment_min_ra_avg", Long.toString(info.raAvgMin));
            usageMetadata.put("ra_size_segment_max_ra_avg", Long.toString(info.raAvgMax));
            usageMetadata.put("ra_size_segment_avg_ra_avg", Long.toString(avg));
            usageMetadata.put("ra_size_segment_stddev_ra_avg", Long.toString(stddev));
        }
    }

    private static final class RaStorageInfo {
        private Instant indexCreationDate;
        private long raStorageSize;

        private long segmentCount;
        private long liveDocCount;
        private long deletedDocCount;

        private long raSegmentCount;
        private long raLiveDocCount;
        private long raDeletedDocCount;
        private long raApproximatedDocCount;
        private long raAvgMin = Long.MAX_VALUE;
        private long raAvgMax = 0;
        private double raAvgTotal;
        private double raAvgSquaredTotal;

        private boolean hasRAStats = false;

        void accumulate(Map.Entry<SampledClusterMetricsService.ShardKey, SampledClusterMetricsService.ShardSample> t) {
            raStorageSize += t.getValue().shardInfo().rawStoredSizeInBytes();

            segmentCount += t.getValue().shardInfo().segmentCount();
            liveDocCount += t.getValue().shardInfo().docCount();
            deletedDocCount += t.getValue().shardInfo().deletedDocCount();

            if (t.getValue().shardInfo().rawStoredSizeStats().isEmpty() == false) {
                hasRAStats = true;
                raSegmentCount += t.getValue().shardInfo().rawStoredSizeStats().segmentCount();
                raLiveDocCount += t.getValue().shardInfo().rawStoredSizeStats().liveDocCount();
                raDeletedDocCount += t.getValue().shardInfo().rawStoredSizeStats().deletedDocCount();
                raApproximatedDocCount += t.getValue().shardInfo().rawStoredSizeStats().approximatedDocCount();
                raAvgMin = Math.min(raAvgMin, t.getValue().shardInfo().rawStoredSizeStats().avgMin());
                raAvgMax = Math.max(raAvgMax, t.getValue().shardInfo().rawStoredSizeStats().avgMax());
                raAvgTotal += t.getValue().shardInfo().rawStoredSizeStats().avgTotal();
                raAvgSquaredTotal += t.getValue().shardInfo().rawStoredSizeStats().avgSquaredTotal();
            }

            indexCreationDate = getEarlierValidCreationDate(
                indexCreationDate,
                Instant.ofEpochMilli(t.getValue().shardInfo().indexCreationDateEpochMilli())
            );
        }

        RaStorageInfo combine(RaStorageInfo b) {
            raStorageSize += b.raStorageSize;

            segmentCount += b.segmentCount;
            liveDocCount += b.liveDocCount;
            deletedDocCount += b.deletedDocCount;

            raSegmentCount += b.raSegmentCount;
            raLiveDocCount += b.raLiveDocCount;
            raDeletedDocCount += b.raDeletedDocCount;
            raApproximatedDocCount += b.raApproximatedDocCount;
            raAvgMin = Math.min(raAvgMin, b.raAvgMin);
            raAvgMax = Math.max(raAvgMax, b.raAvgMax);
            raAvgTotal += b.raAvgTotal;
            raAvgSquaredTotal += b.raAvgSquaredTotal;

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
