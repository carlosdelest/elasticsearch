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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

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

import static co.elastic.elasticsearch.metering.action.utils.PersistentTaskUtils.findPersistentTaskNodeId;
import static org.elasticsearch.core.Strings.format;

public class MeteringIndexInfoService {
    private static final Logger logger = LogManager.getLogger(MeteringIndexInfoService.class);

    static final String NODE_INFO_COLLECTIONS_TOTAL = "es.metering.node_info.collections.total";
    static final String NODE_INFO_COLLECTIONS_ERRORS_TOTAL = "es.metering.node_info.collections.error.total";
    static final String NODE_INFO_COLLECTIONS_PARTIALS_TOTAL = "es.metering.node_info.collections.partial.total";

    private final ClusterService clusterService;
    private final LongCounter collectionsTotalCounter;
    private final LongCounter collectionsErrorsCounter;
    private final LongCounter collectionsPartialsCounter;

    public MeteringIndexInfoService(ClusterService clusterService, MeterRegistry meterRegistry) {
        this.clusterService = clusterService;
        clusterService.addListener(this::clusterChanged);

        this.collectionsTotalCounter = meterRegistry.registerLongCounter(
            NODE_INFO_COLLECTIONS_TOTAL,
            "The total number of scatter operations to gather shard infos from all nodes in the cluster",
            "unit"
        );
        this.collectionsErrorsCounter = meterRegistry.registerLongCounter(
            NODE_INFO_COLLECTIONS_ERRORS_TOTAL,
            "The total number of scatter operations that resulted in an error",
            "unit"
        );
        this.collectionsPartialsCounter = meterRegistry.registerLongCounter(
            NODE_INFO_COLLECTIONS_PARTIALS_TOTAL,
            "The number of scatter operations that resulted in partial information (error/no response from one or more nodes)",
            "unit"
        );
    }

    enum CollectedMeteringShardInfoFlag {
        PARTIAL,
        STALE
    }

    record ShardInfoKey(String indexName, int shardId) {
        @Override
        public String toString() {
            return indexName + ":" + shardId;
        }

        public static ShardInfoKey fromShardId(ShardId shardId) {
            return new ShardInfoKey(shardId.getIndexName(), shardId.id());
        }
    }

    record ShardInfoValue(
        long sizeInBytes,
        long docCount,
        long storedIngestSizeInBytes,
        String indexUUID,
        long primaryTerm,
        long generation
    ) implements ShardEra {
        public static final ShardInfoValue EMPTY = new ShardInfoValue(0, 0, 0, null, 0, 0);
    }

    record CollectedMeteringShardInfo(
        Map<ShardInfoKey, ShardInfoValue> meteringShardInfoMap,
        Set<CollectedMeteringShardInfoFlag> meteringShardInfoStatus
    ) {
        static final CollectedMeteringShardInfo EMPTY = new CollectedMeteringShardInfo(
            Map.of(),
            Set.of(CollectedMeteringShardInfoFlag.STALE)
        );

        ShardInfoValue getShardInfo(ShardInfoKey shardInfoKey) {
            var shardInfo = meteringShardInfoMap.get(shardInfoKey);
            return Objects.requireNonNullElse(shardInfo, ShardInfoValue.EMPTY);
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
     * Package-private for testing.
     */
    void clusterChanged(ClusterChangedEvent event) {
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
        collectionsTotalCounter.increment();
        // If we get called and ask to update, that request comes from the PersistentTask, so we are definitely on
        // the PersistentTask node
        persistentTaskNodeStatus = PersistentTaskNodeStatus.THIS_NODE;
        client.execute(CollectMeteringShardInfoAction.INSTANCE, new CollectMeteringShardInfoAction.Request(), new ActionListener<>() {
            @Override
            public void onResponse(CollectMeteringShardInfoAction.Response response) {
                Set<CollectedMeteringShardInfoFlag> status = EnumSet.noneOf(CollectedMeteringShardInfoFlag.class);
                if (response.isComplete() == false) {
                    collectionsPartialsCounter.increment();
                    status.add(CollectedMeteringShardInfoFlag.PARTIAL);
                }

                // Create a new MeteringShardInfo from diffs.
                collectedShardInfo.getAndUpdate(
                    current -> mergeShardInfo(removeStaleEntries(current.meteringShardInfoMap()), response.getShardInfo(), status)
                );
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
                collectionsErrorsCounter.increment();
            }
        });
    }

    private Map<ShardInfoKey, ShardInfoValue> removeStaleEntries(Map<ShardInfoKey, ShardInfoValue> shardInfoKeyShardInfoValueMap) {
        Set<ShardInfoKey> activeShards = clusterService.state()
            .routingTable()
            .allShards()
            .map(x -> ShardInfoKey.fromShardId(x.shardId()))
            .collect(Collectors.toUnmodifiableSet());

        return shardInfoKeyShardInfoValueMap.entrySet()
            .stream()
            .filter(e -> activeShards.contains(e.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    class StorageInfoMetricsCollector implements SampledMetricsCollector {
        private static final String IX_METRIC_TYPE = "es_indexed_data";
        static final String IX_METRIC_ID_PREFIX = "shard-size";
        private static final String RA_S_METRIC_TYPE = "es_raw_stored_data";
        static final String RA_S_METRIC_ID_PREFIX = "raw-stored-index-size";
        private static final String PARTIAL = "partial";
        private static final String INDEX = "index";
        private static final String SHARD = "shard";

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

            boolean partial = currentInfo.meteringShardInfoStatus().contains(CollectedMeteringShardInfoFlag.PARTIAL);
            List<MetricValue> metrics = new ArrayList<>();
            for (final var shardEntry : currentInfo.meteringShardInfoMap.entrySet()) {
                long size = shardEntry.getValue().sizeInBytes();
                // Do not generate records with size 0
                if (size > 0) {
                    int shardId = shardEntry.getKey().shardId();
                    var indexName = shardEntry.getKey().indexName();

                    Map<String, String> metadata = new HashMap<>();
                    metadata.put(INDEX, indexName);
                    metadata.put(SHARD, Integer.toString(shardId));
                    if (partial) {
                        metadata.put(PARTIAL, Boolean.TRUE.toString());
                    }

                    metrics.add(new MetricValue(format("%s:%s", IX_METRIC_ID_PREFIX, shardEntry.getKey()), IX_METRIC_TYPE, metadata, size));
                }
            }

            Map<String, Long> storedIngestSizeInBytesMap = currentInfo.meteringShardInfoMap.entrySet()
                .stream()
                .collect(
                    Collectors.groupingBy(e -> e.getKey().indexName(), Collectors.summingLong(e -> e.getValue().storedIngestSizeInBytes()))
                );
            for (final var indexEntry : storedIngestSizeInBytesMap.entrySet()) {
                final var indexName = indexEntry.getKey();
                final var storedIngestSizeInBytes = indexEntry.getValue();

                if (storedIngestSizeInBytes > 0) {
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
                            storedIngestSizeInBytes
                        )
                    );
                }
            }
            return Optional.of(SampledMetricsCollector.valuesFromCollection(Collections.unmodifiableCollection(metrics)));
        }
    }

    public SampledMetricsCollector createIndexSizeMetricsCollector() {
        return new StorageInfoMetricsCollector();
    }

    private static CollectedMeteringShardInfo mergeShardInfo(
        Map<ShardInfoKey, ShardInfoValue> current,
        Map<ShardId, MeteringShardInfo> updated,
        Set<CollectedMeteringShardInfoFlag> status
    ) {
        HashMap<ShardInfoKey, ShardInfoValue> map = new HashMap<>(current);
        for (var newEntry : updated.entrySet()) {
            var info = newEntry.getValue();
            var shardKey = ShardInfoKey.fromShardId(newEntry.getKey());
            var shardValue = new ShardInfoValue(
                info.sizeInBytes(),
                info.docCount(),
                info.storedIngestSizeInBytes(),
                newEntry.getKey().getIndex().getUUID(),
                info.primaryTerm(),
                info.generation()
            );
            map.merge(shardKey, shardValue, (oldValue, newValue) -> {
                // In case there is a conflict, we need the index UUID from the original map value to decide what to do
                var originalMapValue = current.get(shardKey);
                if (originalMapValue == null) {
                    // We have no entry from the current map; entries in the "updated" map cannot have the same UUID (this has been already
                    // taken care of at Transport level), so we'll choose one "at random" (and eventually we'll get this correct at the
                    // next round)
                    return newValue;
                } else {
                    // oldValue is either the original value, or a value from the "updated" map we used to replace the original value

                    // Same UUID -> compare mostRecent
                    if (oldValue.indexUUID().equals(newValue.indexUUID())) {
                        return ShardEra.mostRecent(oldValue, newValue);
                    }

                    // oldValue and newValue have different UUIDs
                    // "Do not go back" if newValue has the same UUID as the original
                    if (newValue.indexUUID().equals(originalMapValue.indexUUID())) {
                        return oldValue;
                    }
                    // We assume that a change in UUID means that the value is more recent than the one we already have.
                    return newValue;
                }
            });
        }
        return new CollectedMeteringShardInfo(map, status);
    }

    CollectedMeteringShardInfo getMeteringShardInfo() {
        return collectedShardInfo.get();
    }
}
