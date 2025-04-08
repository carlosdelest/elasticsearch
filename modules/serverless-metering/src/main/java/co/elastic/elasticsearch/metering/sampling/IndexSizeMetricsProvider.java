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

import org.elasticsearch.cluster.metadata.IndexAbstraction;
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

import static org.elasticsearch.core.Strings.format;

class IndexSizeMetricsProvider implements SampledMetricsProvider {
    private static final Logger logger = LogManager.getLogger(IndexSizeMetricsProvider.class);

    static final String IX_METRIC_TYPE = "es_indexed_data";
    static final String IX_SHARD_METRIC_ID_PREFIX = "shard-size";
    static final String IX_INDEX_METRIC_ID_PREFIX = "index-size";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;

    IndexSizeMetricsProvider(
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
        final var indicesLookup = clusterService.state().getMetadata().getProject().getIndicesLookup();
        boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);
        List<MetricValue> metrics = new ArrayList<>();

        // shard-level IX metrics only kept for transition period
        for (final var shardEntry : sample.shardSamples().entrySet()) {
            ShardInfoMetrics shardMetrics = shardEntry.getValue().shardInfo();
            if (shardMetrics.totalSizeInBytes() > 0) {
                metrics.add(ixShardMetric(shardEntry.getKey(), shardMetrics, indicesLookup, partial));
            }
        }

        var indexInfos = IndexInfoMetrics.calculateIndexSamples(sample.shardSamples());
        for (final var indexInfo : indexInfos.entrySet()) {
            if (indexInfo.getValue().getTotalSize() > 0) {
                metrics.add(ixIndexMetric(indexInfo.getKey(), indexInfo.getValue(), indicesLookup, partial));
            }
        }
        return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
    }

    // shard-level IX metrics only kept for transition period
    private MetricValue ixShardMetric(
        ShardKey shard,
        ShardInfoMetrics shardMetrics,
        Map<String, IndexAbstraction> indicesLookup,
        boolean partial
    ) {
        var indexCreationDate = Instant.ofEpochMilli(shardMetrics.indexCreationDateEpochMilli());

        var sourceMetadata = SourceMetadata.indexSourceMetadata(shard.indexName(), indicesLookup, systemIndices, partial);
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

    private MetricValue ixIndexMetric(
        String index,
        IndexInfoMetrics indexInfo,
        Map<String, IndexAbstraction> indicesLookup,
        boolean partial
    ) {
        return new MetricValue(
            format("%s:%s", IX_INDEX_METRIC_ID_PREFIX, index),
            IX_METRIC_TYPE,
            SourceMetadata.indexSourceMetadata(index, indicesLookup, systemIndices, partial),
            ixUsageMetadata(indexInfo),
            indexInfo.getTotalSize(),
            indexInfo.getIndexCreationDate()
        );
    }

    private Map<String, String> ixUsageMetadata(IndexInfoMetrics info) {
        Map<String, String> usageMetadata = Maps.newHashMapWithExpectedSize(4);
        usageMetadata.put("segment_count", Long.toString(info.getSegmentCount()));
        usageMetadata.put("doc_count", Long.toString(info.getLiveDocCount()));
        usageMetadata.put("deleted_doc_count", Long.toString(info.getDeletedDocCount()));
        usageMetadata.put("interactive_size", Long.toString(info.getInteractiveSize()));
        return usageMetadata;
    }
}
