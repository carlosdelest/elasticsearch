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
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    enum PersistentTaskNodeStatus {
        NO_NODE,
        THIS_NODE,
        ANOTHER_NODE
    }

    final AtomicReference<CollectedMeteringShardInfo> collectedShardInfo = new AtomicReference<>(CollectedMeteringShardInfo.EMPTY);
    volatile PersistentTaskNodeStatus persistentTaskNodeStatus = PersistentTaskNodeStatus.NO_NODE;

    /**
     * Monitors cluster state changes to see if we are not the persistent task node anymore.
     * If we are not the persistent task node anymore, reset our cached collected shard info.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var currentPersistentTaskNode = findPersistentTaskNodeId(event.state(), MeteringIndexInfoTask.TASK_NAME);
        var localNode = event.state().nodes().getLocalNodeId();

        var wasPersistentTaskNode = persistentTaskNodeStatus == PersistentTaskNodeStatus.THIS_NODE;
        if (currentPersistentTaskNode == null) {
            persistentTaskNodeStatus = PersistentTaskNodeStatus.NO_NODE;
        } else if (currentPersistentTaskNode.equals(localNode)) {
            persistentTaskNodeStatus = PersistentTaskNodeStatus.THIS_NODE;
        } else {
            persistentTaskNodeStatus = PersistentTaskNodeStatus.ANOTHER_NODE;
        }

        if (persistentTaskNodeStatus != PersistentTaskNodeStatus.THIS_NODE && wasPersistentTaskNode) {
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
        persistentTaskNodeStatus = PersistentTaskNodeStatus.THIS_NODE;
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

    class StorageInfoMetricsCollector implements SampledMetricsCollector {
        private static final String IX_METRIC_TYPE = "es_indexed_data";
        static final String IX_METRIC_ID_PREFIX = "shard-size";
        private static final String RA_S_METRIC_TYPE = "es_raw_stored_data";
        static final String RA_S_METRIC_ID_PREFIX = "raw-stored-index-size";
        private static final String PARTIAL = "partial";
        private static final String INDEX = "index";
        private static final String SHARD = "shard";
        private static final String SEARCH_POWER = "search_power";

        private volatile int searchPowerMinSetting;
        private volatile int searchPowerMaxSetting;

        StorageInfoMetricsCollector(ClusterSettings clusterSettings, Settings settings) {
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
        public Optional<MetricValues> getMetrics() {
            var currentInfo = collectedShardInfo.get();

            if (persistentTaskNodeStatus == PersistentTaskNodeStatus.NO_NODE
                || (currentInfo == CollectedMeteringShardInfo.EMPTY && persistentTaskNodeStatus == PersistentTaskNodeStatus.THIS_NODE)) {
                // We are not ready to return metrics yet
                return Optional.empty();
            }

            if (persistentTaskNodeStatus == PersistentTaskNodeStatus.ANOTHER_NODE || (currentInfo == CollectedMeteringShardInfo.EMPTY)) {
                // We have nothing to report
                return Optional.of(SampledMetricsCollector.NO_VALUES);
            }

            // searchPowerMinSetting to be changed to `searchPowerSelected` when we calculate it.
            Map<String, Object> settings = Map.of(SEARCH_POWER, this.searchPowerMinSetting);

            boolean partial = currentInfo.meteringShardInfoStatus().contains(CollectedMeteringShardInfoFlag.PARTIAL);
            List<MetricValue> metrics = new ArrayList<>();
            for (final var shardEntry : currentInfo.meteringShardInfoMap.entrySet()) {
                int shardId = shardEntry.getKey().id();
                long size = shardEntry.getValue().sizeInBytes();

                var indexName = shardEntry.getKey().getIndexName();

                Map<String, String> metadata = new HashMap<>();
                metadata.put(INDEX, indexName);
                metadata.put(SHARD, Integer.toString(shardId));
                if (partial) {
                    metadata.put(PARTIAL, Boolean.TRUE.toString());
                }

                metrics.add(
                    new MetricValue(format("%s:%s:%s", IX_METRIC_ID_PREFIX, indexName, shardId), IX_METRIC_TYPE, metadata, settings, size)
                );
            }

            Map<String, Optional<Long>> storedIngestSizeInBytesMap = currentInfo.meteringShardInfoMap.entrySet()
                .stream()
                .collect(
                    Collectors.groupingBy(
                        e -> e.getKey().getIndexName(),
                        Collectors.filtering(
                            e -> e.getValue().storedIngestSizeInBytes() != null,
                            Collectors.mapping(e -> e.getValue().storedIngestSizeInBytes(), Collectors.reducing(Long::sum))
                        )
                    )
                );
            for (final var indexEntry : storedIngestSizeInBytesMap.entrySet()) {
                final var indexName = indexEntry.getKey();
                final var storedIngestSizeInBytes = indexEntry.getValue();

                if (storedIngestSizeInBytes.isPresent()) {
                    Map<String, String> metadata = new HashMap<>();
                    metadata.put(INDEX, indexName);
                    if (partial) {
                        metadata.put(PARTIAL, Boolean.TRUE.toString());
                    }
                    metrics.add(
                        new MetricValue(
                            format("%s:%s", RA_S_METRIC_ID_PREFIX, indexName),
                            RA_S_METRIC_TYPE,
                            metadata,
                            settings,
                            storedIngestSizeInBytes.get()
                        )
                    );
                }
            }
            return Optional.of(SampledMetricsCollector.valuesFromCollection(Collections.unmodifiableCollection(metrics)));
        }
    }

    public SampledMetricsCollector createIndexSizeMetricsCollector(ClusterService clusterService, Settings settings) {
        return new StorageInfoMetricsCollector(clusterService.getClusterSettings(), settings);
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
