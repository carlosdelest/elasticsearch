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

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.elasticsearch.core.Strings.format;

/**
 * Responsible for the index size metric.
 * <p>
 * Registers a metric on the metering service,
 * and connects to the SearchService to gather segment sizes whenever it is notified by the gauge metric
 * that a new metric is required.
 */
class IndexSizeMetricsCollector implements MetricsCollector {
    private static final Logger logger = LogManager.getLogger(IndexSizeMetricsCollector.class);
    public static final String METRIC_TYPE = "es_indexed_data";
    private static final String PARTIAL = "partial";
    private static final String INDEX = "index";
    private static final String SHARD = "shard";
    private static final String SEARCH_POWER = "search_power";
    final IndicesService indicesService;
    private volatile int searchPowerSetting;

    IndexSizeMetricsCollector(IndicesService indicesService, ClusterSettings clusterSettings, Settings settings) {
        this.indicesService = indicesService;
        this.searchPowerSetting = SEARCH_POWER_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_POWER_SETTING, sp -> this.searchPowerSetting = sp);
    }

    @Override
    public Collection<MetricValue> getMetrics() {
        Map<String, Object> settings = Map.of(SEARCH_POWER, searchPowerSetting);

        List<MetricValue> metrics = new ArrayList<>();
        for (final IndexService indexService : indicesService) {
            String indexName = indexService.index().getName();
            for (final IndexShard shard : indexService) {

                Engine engine = shard.getEngineOrNull();
                if (engine == null || shard.isSystem()) {
                    continue;
                }

                SegmentInfos segmentInfos = engine.getLastCommittedSegmentInfos();
                if (segmentInfos.size() == 0) {
                    continue;
                }

                int shardId = shard.shardId().id();
                long size = 0;
                boolean partial = false;
                for (SegmentCommitInfo si : segmentInfos) {
                    try {
                        long commitSize = si.sizeInBytes();
                        size += commitSize;
                    } catch (IOException err) {
                        partial = true;
                        logger.warn(
                            "Failed to read file size for shard: [{}], commitId: [{}], err: [{}]",
                            shardId,
                            StringHelper.idToString(si.getId()),
                            err
                        );
                    }
                }

                Map<String, String> metadata = new HashMap<>();
                metadata.put(INDEX, indexName);
                metadata.put(SHARD, Integer.toString(shardId));
                if (partial) {
                    metadata.put(PARTIAL, Boolean.TRUE.toString());
                }
                String metricId = format("shard-size:%s:%s", indexName, shardId);

                metrics.add(new MetricValue(MeasurementType.SAMPLED, metricId, METRIC_TYPE, metadata, settings, size));
            }
        }
        return metrics;
    }

}
