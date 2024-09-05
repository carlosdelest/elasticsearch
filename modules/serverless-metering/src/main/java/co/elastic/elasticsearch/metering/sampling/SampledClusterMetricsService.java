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

import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

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

import java.time.Instant;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.sampling.utils.PersistentTaskUtils.findPersistentTaskNodeId;
import static org.elasticsearch.core.Strings.format;

public class SampledClusterMetricsService {
    private static final Logger logger = LogManager.getLogger(SampledClusterMetricsService.class);

    static final String NODE_INFO_COLLECTIONS_TOTAL = "es.metering.node_info.collections.total";
    static final String NODE_INFO_COLLECTIONS_ERRORS_TOTAL = "es.metering.node_info.collections.error.total";
    static final String NODE_INFO_COLLECTIONS_PARTIALS_TOTAL = "es.metering.node_info.collections.partial.total";

    private final ClusterService clusterService;
    private final LongCounter collectionsTotalCounter;
    private final LongCounter collectionsErrorsCounter;
    private final LongCounter collectionsPartialsCounter;

    public SampledClusterMetricsService(ClusterService clusterService, MeterRegistry meterRegistry) {
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

    public interface SampledShardInfos {
        ShardInfoMetrics get(ShardId shardId);
    }

    enum SamplingStatus {
        /** Partial samples, at least 1 node failed to respond. */
        PARTIAL,
        /** Stale data, the last attempt to collect samples failed. */
        STALE
    }

    record ShardKey(String indexName, int shardId) {
        @Override
        public String toString() {
            return indexName + ":" + shardId;
        }

        static ShardKey fromShardId(ShardId shardId) {
            return new ShardKey(shardId.getIndexName(), shardId.id());
        }
    }

    record ShardSample(String indexUUID, ShardInfoMetrics shardInfo) {
        static final ShardSample EMPTY = new ShardSample(null, ShardInfoMetrics.EMPTY);
    }

    record SampledClusterMetrics(Map<ShardKey, ShardSample> shardSamples, Set<SamplingStatus> status) implements SampledShardInfos {
        static final SampledClusterMetrics EMPTY = new SampledClusterMetrics(Map.of(), Set.of(SamplingStatus.STALE));

        public ShardInfoMetrics get(ShardId shardId) {
            var shardInfo = shardSamples.get(ShardKey.fromShardId(shardId));
            return Objects.requireNonNullElse(shardInfo, ShardSample.EMPTY).shardInfo;
        }
    }

    enum PersistentTaskNodeStatus {
        NO_NODE,
        THIS_NODE,
        ANOTHER_NODE
    }

    final AtomicReference<SampledClusterMetrics> collectedShardInfo = new AtomicReference<>(SampledClusterMetrics.EMPTY);
    volatile PersistentTaskNodeStatus persistentTaskNodeStatus = PersistentTaskNodeStatus.NO_NODE;

    /**
     * Monitors cluster state changes to see if we are not the persistent task node anymore.
     * If we are not the persistent task node anymore, reset our cached collected shard info.
     * Package-private for testing.
     */
    void clusterChanged(ClusterChangedEvent event) {
        var currentPersistentTaskNode = findPersistentTaskNodeId(event.state(), SampledClusterMetricsSchedulingTask.TASK_NAME);
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
            collectedShardInfo.set(SampledClusterMetrics.EMPTY);
        }
    }

    /**
     * Updates the internal storage of metering shard info, by performing a scatter-gather operation towards all (search) nodes
     */
    void updateSamples(Client client) {
        logger.debug("Calling IndexSizeService#updateMeteringShardInfo");
        collectionsTotalCounter.increment();
        // If we get called and ask to update, that request comes from the PersistentTask, so we are definitely on
        // the PersistentTask node
        persistentTaskNodeStatus = PersistentTaskNodeStatus.THIS_NODE;
        client.execute(CollectClusterSamplesAction.INSTANCE, new CollectClusterSamplesAction.Request(), new ActionListener<>() {
            @Override
            public void onResponse(CollectClusterSamplesAction.Response response) {
                Set<SamplingStatus> status = EnumSet.noneOf(SamplingStatus.class);
                if (response.isComplete() == false) {
                    collectionsPartialsCounter.increment();
                    status.add(SamplingStatus.PARTIAL);
                }

                // Create a new MeteringShardInfo from diffs.
                collectedShardInfo.getAndUpdate(
                    current -> mergeShardInfos(removeStaleEntries(current.shardSamples()), response.getShardInfos(), status)
                );
                logger.debug(
                    () -> Strings.format(
                        "collected new metering shard info for shards [%s]",
                        response.getShardInfos().keySet().stream().map(ShardId::toString).collect(Collectors.joining(","))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                var previousSizes = collectedShardInfo.get();
                var status = EnumSet.copyOf(previousSizes.status());
                status.add(SamplingStatus.STALE);
                collectedShardInfo.set(new SampledClusterMetrics(previousSizes.shardSamples(), status));
                logger.error("failed to collect metering shard info", e);
                collectionsErrorsCounter.increment();
            }
        });
    }

    private Map<ShardKey, ShardSample> removeStaleEntries(Map<ShardKey, ShardSample> shardInfoKeyShardInfoValueMap) {
        Set<ShardKey> activeShards = clusterService.state()
            .routingTable()
            .allShards()
            .map(x -> ShardKey.fromShardId(x.shardId()))
            .collect(Collectors.toUnmodifiableSet());

        return shardInfoKeyShardInfoValueMap.entrySet()
            .stream()
            .filter(e -> activeShards.contains(e.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // TODO move this to a separate class
    class SampledStorageMetricsProvider implements SampledMetricsProvider {
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
                || (currentInfo == SampledClusterMetrics.EMPTY && persistentTaskNodeStatus == PersistentTaskNodeStatus.THIS_NODE)) {
                // We are not ready to return metrics yet
                return Optional.empty();
            }

            if (persistentTaskNodeStatus == PersistentTaskNodeStatus.ANOTHER_NODE || (currentInfo == SampledClusterMetrics.EMPTY)) {
                // We have nothing to report
                return Optional.of(SampledMetricsProvider.NO_VALUES);
            }

            boolean partial = currentInfo.status().contains(SamplingStatus.PARTIAL);
            List<MetricValue> metrics = new ArrayList<>();
            for (final var shardEntry : currentInfo.shardSamples.entrySet()) {
                long size = shardEntry.getValue().shardInfo().sizeInBytes();
                // Do not generate records with size 0
                if (size > 0) {
                    int shardId = shardEntry.getKey().shardId();
                    var indexName = shardEntry.getKey().indexName();
                    var indexCreationDate = Instant.ofEpochMilli(shardEntry.getValue().shardInfo().indexCreationDateEpochMilli());

                    Map<String, String> metadata = new HashMap<>();
                    metadata.put(INDEX, indexName);
                    metadata.put(SHARD, Integer.toString(shardId));
                    if (partial) {
                        metadata.put(PARTIAL, Boolean.TRUE.toString());
                    }

                    metrics.add(
                        new MetricValue(
                            format("%s:%s", IX_METRIC_ID_PREFIX, shardEntry.getKey()),
                            IX_METRIC_TYPE,
                            metadata,
                            size,
                            indexCreationDate
                        )
                    );
                }
            }

            Map<String, RaStorageInfo> raStorageInfos = currentInfo.shardSamples.entrySet()
                .stream()
                .collect(
                    Collectors.groupingBy(
                        e -> e.getKey().indexName(),
                        Collector.of(RaStorageInfo::new, RaStorageInfo::accumulate, RaStorageInfo::combine)
                    )
                );
            for (final var indexEntry : raStorageInfos.entrySet()) {
                final var indexName = indexEntry.getKey();
                final var storedIngestSizeInBytes = indexEntry.getValue().raStorageSize;
                final var indexCreationDate = indexEntry.getValue().indexCreationDate;

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
                            storedIngestSizeInBytes,
                            indexCreationDate
                        )
                    );
                }
            }
            return Optional.of(SampledMetricsProvider.valuesFromCollection(Collections.unmodifiableCollection(metrics)));
        }

        private static final class RaStorageInfo {
            private Instant indexCreationDate;
            private long raStorageSize;

            void accumulate(Map.Entry<ShardKey, ShardSample> t) {
                raStorageSize += t.getValue().shardInfo().storedIngestSizeInBytes();
                indexCreationDate = getEarlierValidCreationDate(
                    indexCreationDate,
                    Instant.ofEpochMilli(t.getValue().shardInfo.indexCreationDateEpochMilli())
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

    public SampledMetricsProvider createSampledStorageMetricsProvider() {
        return new SampledStorageMetricsProvider();
    }

    private static SampledClusterMetrics mergeShardInfos(
        Map<ShardKey, ShardSample> current,
        Map<ShardId, ShardInfoMetrics> updated,
        Set<SamplingStatus> status
    ) {
        HashMap<ShardKey, ShardSample> map = new HashMap<>(current);
        for (var newEntry : updated.entrySet()) {
            var shardKey = ShardKey.fromShardId(newEntry.getKey());
            var shardValue = new ShardSample(newEntry.getKey().getIndex().getUUID(), newEntry.getValue());

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
                        return oldValue.shardInfo.isMoreRecentThan(newValue.shardInfo) ? oldValue : newValue;
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
        return new SampledClusterMetrics(map, status);
    }

    public SampledShardInfos getMeteringShardInfo() {
        return collectedShardInfo.get();
    }
}
