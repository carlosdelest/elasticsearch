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

import co.elastic.elasticsearch.metering.UsageMetadata;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

class RawStorageMetricsProvider implements SampledMetricsProvider {
    private static final Logger logger = LogManager.getLogger(RawStorageMetricsProvider.class);

    static final String RA_S_METRIC_TYPE = "es_raw_stored_data";
    static final String RA_S_METRIC_ID_PREFIX = "raw-stored-index-size";

    private final SampledClusterMetricsService sampledClusterMetricsService;

    RawStorageMetricsProvider(SampledClusterMetricsService sampledClusterMetricsService) {
        this.sampledClusterMetricsService = sampledClusterMetricsService;
    }

    @Override
    public Optional<MetricValues> getMetrics() {
        return sampledClusterMetricsService.withSamplesIfReady(
            this::sampleToMetricValues,
            status -> logger.warn("Samples not ready metrics collection [sampling node: {}]", status)
        );
    }

    private MetricValues sampleToMetricValues(SampledClusterMetricsService.SampledClusterMetrics sample) {
        boolean partial = sample.status().contains(SampledClusterMetricsService.SamplingStatus.PARTIAL);

        var indexInfos = sample.storageMetrics().getIndexInfos();
        List<MetricValue> metrics = new ArrayList<>(indexInfos.size());
        for (final var indexInfo : indexInfos.entrySet()) {
            if (indexInfo.getValue().getRawStorageSize() > 0) {
                metrics.add(rawStorageIndexMetric(indexInfo.getKey(), indexInfo.getValue(), partial));
            }
        }
        return SampledMetricsProvider.metricValues(metrics, DefaultSampledMetricsBackfillStrategy.INSTANCE);
    }

    private MetricValue rawStorageIndexMetric(Index index, IndexInfoMetrics indexInfo, boolean partial) {
        return new MetricValue(
            format("%s:%s", RA_S_METRIC_ID_PREFIX, index.getUUID()),
            RA_S_METRIC_TYPE,
            indexInfo.getSourceMetadata(),
            rasUsageMetadata(indexInfo, partial),
            indexInfo.getRawStorageSize(),
            indexInfo.getIndexCreationDate()
        );
    }

    private Map<String, String> rasUsageMetadata(IndexInfoMetrics info, boolean partial) {
        Map<String, String> usageMetadata = new HashMap<>();
        if (partial) {
            usageMetadata.put(UsageMetadata.PARTIAL, Boolean.TRUE.toString());
        }
        usageMetadata.put("segment_count", Long.toString(info.getSegmentCount()));
        usageMetadata.put("doc_count", Long.toString(info.getLiveDocCount()));
        usageMetadata.put("deleted_doc_count", Long.toString(info.getDeletedDocCount()));

        if (info.hasRawStats()) {
            usageMetadata.put("ra_size_segment_count", Long.toString(info.getRawSegmentCount()));
            usageMetadata.put("ra_size_doc_count", Long.toString(info.getRawLiveDocCount()));
            usageMetadata.put("ra_size_deleted_doc_count", Long.toString(info.getRawDeletedDocCount()));
            usageMetadata.put("ra_size_approximated_doc_count", Long.toString(info.getRawApproximatedDocCount()));
            usageMetadata.put("ra_size_segment_min_ra_avg", Long.toString(info.getRawAvgMin()));
            usageMetadata.put("ra_size_segment_max_ra_avg", Long.toString(info.getRawAvgMax()));
            usageMetadata.put("ra_size_segment_avg_ra_avg", Long.toString(info.getRawAvgAvg()));
            usageMetadata.put("ra_size_segment_stddev_ra_avg", Long.toString(info.getRawAvgStddev()));
        }
        return usageMetadata;
    }

}
