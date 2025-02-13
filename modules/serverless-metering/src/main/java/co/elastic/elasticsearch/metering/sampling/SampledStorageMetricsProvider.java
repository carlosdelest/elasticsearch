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
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.ShardKey;
import co.elastic.elasticsearch.metering.usagereports.DefaultSampledMetricsBackfillStrategy;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.core.Strings.format;

class SampledStorageMetricsProvider implements SampledMetricsProvider {
    private static final Logger logger = LogManager.getLogger(SampledStorageMetricsProvider.class);

    static final String IX_METRIC_TYPE = "es_indexed_data";
    static final String RA_S_METRIC_TYPE = "es_raw_stored_data";

    static final String IX_SHARD_METRIC_ID_PREFIX = "shard-size";
    static final String IX_INDEX_METRIC_ID_PREFIX = "index-size";
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
        return sampledClusterMetricsService.withSamplesIfReady(
            this::sampleToMetricValues,
            status -> logger.warn("Samples not ready metrics collection [sampling node: {}]", status)
        );
    }

    private MetricValues sampleToMetricValues(SampledClusterMetricsService.SampledClusterMetrics sample) {
        final var stateMetadata = clusterService.state().getMetadata();
        boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
        List<MetricValue> metrics = new ArrayList<>();

        // shard-level IX metrics only kept for transition period
        for (final var shardEntry : sample.shardSamples().entrySet()) {
            ShardInfoMetrics shardMetrics = shardEntry.getValue().shardInfo();
            if (shardMetrics.totalSizeInBytes() > 0) {
                metrics.add(ixShardMetric(shardEntry.getKey(), shardMetrics, stateMetadata, partial));
            }
        }

        var indexInfoCollector = Collector.of(IndexInfo::new, IndexInfo::accumulate, IndexInfo::combine);
        var indexInfos = sample.shardSamples().entrySet().stream().collect(groupingBy(e -> e.getKey().indexName(), indexInfoCollector));

        for (final var indexInfo : indexInfos.entrySet()) {
            if (indexInfo.getValue().totalSize > 0) {
                metrics.add(ixIndexMetric(indexInfo.getKey(), indexInfo.getValue(), stateMetadata, partial));
            }
            if (indexInfo.getValue().raStorageSize > 0) {
                metrics.add(raStorageIndexMetric(indexInfo.getKey(), indexInfo.getValue(), stateMetadata, partial));
            }
        }
        return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
    }

    // shard-level IX metrics only kept for transition period
    private MetricValue ixShardMetric(ShardKey shard, ShardInfoMetrics shardMetrics, Metadata metadata, boolean partial) {
        var indexCreationDate = Instant.ofEpochMilli(shardMetrics.indexCreationDateEpochMilli());

        var sourceMetadata = indexSourceMetadata(metadata, shard.indexName(), partial);
        sourceMetadata.put(SourceMetadata.SHARD, Integer.toString(shard.shardId()));

        Map<String, String> usageMetadata = new HashMap<>();
        usageMetadata.put("segment_count", Long.toString(shardMetrics.segmentCount()));
        usageMetadata.put("doc_count", Long.toString(shardMetrics.docCount()));
        usageMetadata.put("deleted_doc_count", Long.toString(shardMetrics.deletedDocCount()));
        usageMetadata.put("interactive_size", Long.toString(shardMetrics.interactiveSizeInBytes()));

        return new MetricValue(
            format("%s:%s", IX_SHARD_METRIC_ID_PREFIX, shard),
            IX_METRIC_TYPE,
            sourceMetadata,
            usageMetadata,
            shardMetrics.totalSizeInBytes(),
            indexCreationDate
        );
    }

    private MetricValue ixIndexMetric(String index, IndexInfo indexInfo, Metadata metadata, boolean partial) {
        return new MetricValue(
            format("%s:%s", IX_INDEX_METRIC_ID_PREFIX, index),
            IX_METRIC_TYPE,
            indexSourceMetadata(metadata, index, partial),
            ixUsageMetadata(indexInfo),
            indexInfo.totalSize,
            indexInfo.indexCreationDate
        );
    }

    private MetricValue raStorageIndexMetric(String indexName, IndexInfo indexInfo, Metadata metadata, boolean partial) {
        return new MetricValue(
            format("%s:%s", RA_S_METRIC_ID_PREFIX, indexName),
            RA_S_METRIC_TYPE,
            indexSourceMetadata(metadata, indexName, partial),
            rasUsageMetadata(indexInfo),
            indexInfo.raStorageSize,
            indexInfo.indexCreationDate
        );
    }

    private Map<String, String> indexSourceMetadata(Metadata clusterMetadata, String indexName, boolean partial) {
        // note: this is intentionally not resolved via IndexAbstraction, see https://elasticco.atlassian.net/browse/ES-10384
        final var isSystemIndex = systemIndices.isSystemIndex(indexName);
        final var indexAbstraction = clusterMetadata.getIndicesLookup().get(indexName);
        final var datastream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;

        Map<String, String> sourceMetadata = Maps.newHashMapWithExpectedSize(4);
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
        return sourceMetadata;
    }

    private void fillIndexUsageMetadata(Map<String, String> usageMetadata, IndexInfo info) {
        usageMetadata.put("segment_count", Long.toString(info.segmentCount));
        usageMetadata.put("doc_count", Long.toString(info.liveDocCount));
        usageMetadata.put("deleted_doc_count", Long.toString(info.deletedDocCount));
    }

    private Map<String, String> ixUsageMetadata(IndexInfo info) {
        Map<String, String> usageMetadata = Maps.newHashMapWithExpectedSize(4);
        fillIndexUsageMetadata(usageMetadata, info);
        usageMetadata.put("interactive_size", Long.toString(info.interactiveSize));
        return usageMetadata;
    }

    private Map<String, String> rasUsageMetadata(IndexInfo info) {
        Map<String, String> usageMetadata = Maps.newHashMapWithExpectedSize(info.hasRAStats ? 3 + 8 : 3);
        fillIndexUsageMetadata(usageMetadata, info);
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
        return usageMetadata;
    }

    private static final class IndexInfo {
        private Instant indexCreationDate;
        private long totalSize;
        private long interactiveSize;

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

        void accumulate(Map.Entry<ShardKey, SampledClusterMetricsService.ShardSample> t) {
            var shardInfo = t.getValue().shardInfo();

            totalSize += shardInfo.totalSizeInBytes();
            interactiveSize += shardInfo.interactiveSizeInBytes();
            raStorageSize += shardInfo.rawStoredSizeInBytes();

            segmentCount += shardInfo.segmentCount();
            liveDocCount += shardInfo.docCount();
            deletedDocCount += shardInfo.deletedDocCount();

            if (shardInfo.rawStoredSizeStats().isEmpty() == false) {
                hasRAStats = true;
                raSegmentCount += shardInfo.rawStoredSizeStats().segmentCount();
                raLiveDocCount += shardInfo.rawStoredSizeStats().liveDocCount();
                raDeletedDocCount += shardInfo.rawStoredSizeStats().deletedDocCount();
                raApproximatedDocCount += shardInfo.rawStoredSizeStats().approximatedDocCount();
                raAvgMin = Math.min(raAvgMin, shardInfo.rawStoredSizeStats().avgMin());
                raAvgMax = Math.max(raAvgMax, shardInfo.rawStoredSizeStats().avgMax());
                raAvgTotal += shardInfo.rawStoredSizeStats().avgTotal();
                raAvgSquaredTotal += shardInfo.rawStoredSizeStats().avgSquaredTotal();
            }

            indexCreationDate = getEarlierValidCreationDate(
                indexCreationDate,
                Instant.ofEpochMilli(shardInfo.indexCreationDateEpochMilli())
            );
        }

        IndexInfo combine(IndexInfo b) {
            totalSize += b.totalSize;
            interactiveSize += b.interactiveSize;
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
