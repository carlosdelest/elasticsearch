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

package co.elastic.elasticsearch.metering.action;

import co.elastic.elasticsearch.metering.MeteringIndexInfoTask;
import co.elastic.elasticsearch.metrics.MetricsCollector;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.metering.action.utils.PersistentTaskUtils.findPersistentTaskNodeId;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.elasticsearch.core.Strings.format;

public class MeteringIndexInfoService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MeteringIndexInfoService.class);

    enum CollectedMeteringShardInfoFlag {
        PARTIAL,
        STALE
    }

    record CollectedMeteringShardInfo(
        Map<ShardId, MeteringShardInfo> meteringShardInfoMap,
        Set<CollectedMeteringShardInfoFlag> meteringShardInfoStatus
    ) {
        static final CollectedMeteringShardInfo EMPTY = new CollectedMeteringShardInfo(
            Map.of(),
            Set.of(CollectedMeteringShardInfoFlag.STALE)
        );

        public MeteringShardInfo getMeteringShardInfoMap(ShardId shardId) {
            var shardInfo = meteringShardInfoMap.get(shardId);
            return Objects.requireNonNullElse(shardInfo, MeteringShardInfo.EMPTY);
        }
    }

    final AtomicReference<CollectedMeteringShardInfo> collectedShardInfo = new AtomicReference<>(CollectedMeteringShardInfo.EMPTY);
    volatile boolean isPersistentTaskNode;

    /**
     * Monitors cluster state changes to see if we are not the persistent task node anymore.
     * If we are not the persistent task node anymore, reset our cached collected shard info.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var currentPersistentTaskNode = findPersistentTaskNodeId(event.state(), MeteringIndexInfoTask.TASK_NAME);
        var localNode = event.state().nodes().getLocalNodeId();

        var wasPersistentTaskNode = isPersistentTaskNode;
        isPersistentTaskNode = currentPersistentTaskNode != null && currentPersistentTaskNode.equals(localNode);

        if (isPersistentTaskNode == false && wasPersistentTaskNode) {
            collectedShardInfo.set(CollectedMeteringShardInfo.EMPTY);
        }
    }

    /**
     * Updates the internal storage of metering shard info, by performing a scatter-gather operation towards all (search) nodes
     */
    public void updateMeteringShardInfo(Client client) {
        logger.debug("Calling IndexSizeService#updateMeteringShardInfo");
        // If we get called and ask to update, that request comes from the PersistentTask, so we are definitely on
        // the PersistentTask node
        isPersistentTaskNode = true;
        client.execute(CollectMeteringShardInfoAction.INSTANCE, new CollectMeteringShardInfoAction.Request(), new ActionListener<>() {
            @Override
            public void onResponse(CollectMeteringShardInfoAction.Response response) {
                Set<CollectedMeteringShardInfoFlag> status = EnumSet.noneOf(CollectedMeteringShardInfoFlag.class);
                if (response.isComplete() == false) {
                    status.add(CollectedMeteringShardInfoFlag.PARTIAL);
                }

                // Create a new MeteringShardInfo from diffs.
                collectedShardInfo.getAndUpdate(current -> mergeShardInfo(current.meteringShardInfoMap(), response.getShardInfo(), status));
                logger.debug(
                    () -> Strings.format(
                        "collected new metering shard info for shards [%s]",
                        response.getShardInfo().keySet().stream().map(ShardId::toString).collect(Collectors.joining(","))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                var previousSizes = collectedShardInfo.get();
                var status = EnumSet.copyOf(previousSizes.meteringShardInfoStatus());
                status.add(CollectedMeteringShardInfoFlag.STALE);
                collectedShardInfo.set(new CollectedMeteringShardInfo(previousSizes.meteringShardInfoMap(), status));
                logger.error("failed to collect metering shard info", e);
            }
        });
    }

    private class IndexSizeMetricsCollector implements MetricsCollector {
        public static final String METRIC_TYPE = "es_indexed_data";
        private static final String PARTIAL = "partial";
        private static final String INDEX = "index";
        private static final String SHARD = "shard";
        private static final String SEARCH_POWER = "search_power";

        private volatile int searchPowerMinSetting;
        private volatile int searchPowerMaxSetting;

        IndexSizeMetricsCollector(ClusterSettings clusterSettings, Settings settings) {
            this.searchPowerMinSetting = SEARCH_POWER_MIN_SETTING.get(settings);
            this.searchPowerMaxSetting = SEARCH_POWER_MAX_SETTING.get(settings);
            clusterSettings.addSettingsUpdateConsumer(SEARCH_POWER_MIN_SETTING, sp -> this.searchPowerMinSetting = sp);
            clusterSettings.addSettingsUpdateConsumer(SEARCH_POWER_MAX_SETTING, sp -> this.searchPowerMaxSetting = sp);
            clusterSettings.addSettingsUpdateConsumer(SEARCH_POWER_SETTING, sp -> {
                if (this.searchPowerMinSetting == this.searchPowerMaxSetting) {
                    this.searchPowerMinSetting = sp;
                    this.searchPowerMaxSetting = sp;
                } else {
                    throw new IllegalArgumentException(
                        "Updating "
                            + ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey()
                            + " ["
                            + sp
                            + "] while "
                            + ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                            + " ["
                            + this.searchPowerMinSetting
                            + "] and "
                            + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                            + " ["
                            + this.searchPowerMaxSetting
                            + "] are not equal."
                    );
                }
            });
        }

        @Override
        public Collection<MetricValue> getMetrics() {

            var currentInfo = collectedShardInfo.get();
            if (currentInfo == CollectedMeteringShardInfo.EMPTY) {
                return Collections.emptyList();
            }

            // searchPowerMinSetting to be changed to `searchPowerSelected` when we calculate it.
            Map<String, Object> settings = Map.of(SEARCH_POWER, this.searchPowerMinSetting);

            List<MetricValue> metrics = new ArrayList<>();
            for (final var shardEntry : currentInfo.meteringShardInfoMap.entrySet()) {
                int shardId = shardEntry.getKey().id();
                long size = shardEntry.getValue().sizeInBytes();
                boolean partial = currentInfo.meteringShardInfoStatus().contains(CollectedMeteringShardInfoFlag.PARTIAL);
                var indexName = shardEntry.getKey().getIndexName();

                Map<String, String> metadata = new HashMap<>();
                metadata.put(INDEX, indexName);
                metadata.put(SHARD, Integer.toString(shardId));
                if (partial) {
                    metadata.put(PARTIAL, Boolean.TRUE.toString());
                }
                String metricId = format("shard-size:%s:%s", indexName, shardId);

                metrics.add(new MetricValue(MeasurementType.SAMPLED, metricId, METRIC_TYPE, metadata, settings, size));
            }

            return metrics;
        }
    }

    public MetricsCollector createIndexSizeMetricsCollector(ClusterService clusterService, Settings settings) {
        return new IndexSizeMetricsCollector(clusterService.getClusterSettings(), settings);
    }

    static CollectedMeteringShardInfo mergeShardInfo(
        Map<ShardId, MeteringShardInfo> current,
        Map<ShardId, MeteringShardInfo> updated,
        Set<CollectedMeteringShardInfoFlag> status
    ) {
        var merged = Stream.concat(current.entrySet().stream(), updated.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TransportCollectMeteringShardInfoAction::mostRecent));
        return new CollectedMeteringShardInfo(merged, status);
    }

    CollectedMeteringShardInfo getMeteringShardInfo() {
        return collectedShardInfo.get();
    }
}
