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
import co.elastic.elasticsearch.metering.UsageMetadata;
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

class IndexSizeMetricsProvider implements SampledMetricsProvider {
    private static final Logger logger = LogManager.getLogger(IndexSizeMetricsProvider.class);

    static final String IX_METRIC_TYPE = "es_indexed_data";
    static final String IX_INDEX_METRIC_ID_PREFIX = "index-size";

    private final SampledClusterMetricsService sampledClusterMetricsService;
    private final SPMinProvisionedMemoryCalculator spMinMemoryCalculator;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;

    IndexSizeMetricsProvider(
        SampledClusterMetricsService sampledClusterMetricsService,
        SPMinProvisionedMemoryCalculator spMinMemoryCalculator,
        ClusterService clusterService,
        SystemIndices systemIndices
    ) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
        this.spMinMemoryCalculator = spMinMemoryCalculator;
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
        final var searchActivity = sampledClusterMetricsService.activitySnapshot(sample.searchTierMetrics());

        boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);

        var indexInfos = IndexInfoMetrics.calculateIndexSamples(sample.shardSamples());
        List<MetricValue> metrics = new ArrayList<>(indexInfos.size());
        for (final var indexInfo : indexInfos.entrySet()) {
            if (indexInfo.getValue().getTotalSize() > 0) {
                metrics.add(ixIndexMetric(indexInfo.getKey(), indexInfo.getValue(), searchActivity, indicesLookup, partial));
            }
        }
        return SampledMetricsProvider.metricValues(
            metrics,
            new IndexSizeMetricsBackfillStrategy(
                sample.searchTierMetrics().activity(),
                sampledClusterMetricsService.activityCoolDownPeriod()
            )
        );
    }

    private MetricValue ixIndexMetric(
        String index,
        IndexInfoMetrics indexInfo,
        Activity.Snapshot searchActivity,
        Map<String, IndexAbstraction> indicesLookup,
        boolean partial
    ) {
        return new MetricValue(
            format("%s:%s", IX_INDEX_METRIC_ID_PREFIX, index),
            IX_METRIC_TYPE,
            SourceMetadata.indexSourceMetadata(index, indicesLookup, systemIndices, partial),
            ixUsageMetadata(indexInfo, searchActivity),
            indexInfo.getTotalSize(),
            indexInfo.getIndexCreationDate()
        );
    }

    private Map<String, String> ixUsageMetadata(IndexInfoMetrics info, Activity.Snapshot searchActivity) {
        Map<String, String> usageMetadata = new HashMap<>();
        usageMetadata.put("segment_count", Long.toString(info.getSegmentCount()));
        usageMetadata.put("doc_count", Long.toString(info.getLiveDocCount()));
        usageMetadata.put("deleted_doc_count", Long.toString(info.getDeletedDocCount()));
        usageMetadata.put("interactive_size", Long.toString(info.getInteractiveSize()));
        updateIxUsageMetadata(
            usageMetadata,
            spMinMemoryCalculator.calculate(info.getInteractiveSize(), info.getTotalSize()),
            searchActivity
        );
        return usageMetadata;
    }

    static void updateIxUsageMetadata(
        Map<String, String> usageMetadata,
        SPMinProvisionedMemoryCalculator.SPMinInfo spMinInfo,
        Activity.Snapshot searchActivity
    ) {
        if (spMinInfo != null) {
            spMinInfo.appendToUsageMetadata(usageMetadata);
        }
        searchActivity.appendToUsageMetadata(
            usageMetadata,
            UsageMetadata.SEARCH_TIER_ACTIVE,
            UsageMetadata.SEARCH_TIER_LATEST_ACTIVITY_TIMESTAMP
        );
    }
}
