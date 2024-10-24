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

import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.metering.sampling.utils.PersistentTaskUtils.findPersistentTaskNodeId;
import static org.elasticsearch.core.Strings.format;

public class SampledClusterMetricsService {
    private static final Logger logger = LogManager.getLogger(SampledClusterMetricsService.class);

    static final String NODE_INFO_COLLECTIONS_TOTAL = "es.metering.node_info.collections.total";
    static final String NODE_INFO_COLLECTIONS_ERRORS_TOTAL = "es.metering.node_info.collections.error.total";
    static final String NODE_INFO_COLLECTIONS_PARTIALS_TOTAL = "es.metering.node_info.collections.partial.total";
    static final String NODE_INFO_TIER_SEARCH_ACTIVITY_TIME = "es.metering.node_info.tier.search.activity.time";
    static final String NODE_INFO_TIER_INDEX_ACTIVITY_TIME = "es.metering.node_info.tier.index.activity.time";
    private final ClusterService clusterService;
    private final Duration coolDown;
    private final MeterRegistry meterRegistry;
    private final LongCounter collectionsTotalCounter;
    private final LongCounter collectionsErrorsCounter;
    private final LongCounter collectionsPartialsCounter;

    @SuppressWarnings("this-escape")
    public SampledClusterMetricsService(ClusterService clusterService, MeterRegistry meterRegistry) {
        this.clusterService = clusterService;
        this.meterRegistry = meterRegistry;

        clusterService.addListener(this::clusterChanged);
        this.coolDown = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());

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
        meterRegistry.registerLongsGauge(
            NODE_INFO_TIER_SEARCH_ACTIVITY_TIME,
            "The seconds since search tier last became active or inactive. Value is positive if active, and negative if inactive",
            "sec",
            () -> this.withSamplesIfReady(sample -> makeActivityMeter(sample.searchTierMetrics())).stream().toList()
        );
        meterRegistry.registerLongsGauge(
            NODE_INFO_TIER_INDEX_ACTIVITY_TIME,
            "The seconds since index tier last became active or inactive. Value is positive if active, and negative if inactive",
            "sec",
            () -> this.withSamplesIfReady(sample -> makeActivityMeter(sample.indexTierMetrics())).stream().toList()
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

    record SampledClusterMetrics(
        SampledTierMetrics searchTierMetrics,
        SampledTierMetrics indexTierMetrics,
        Map<ShardKey, ShardSample> shardSamples,
        Set<SamplingStatus> status
    ) implements SampledShardInfos {

        static final SampledClusterMetrics EMPTY = new SampledClusterMetrics(
            SampledTierMetrics.EMPTY,
            SampledTierMetrics.EMPTY,
            Map.of(),
            Set.of(SamplingStatus.STALE)
        );

        public ShardInfoMetrics get(ShardId shardId) {
            var shardInfo = shardSamples.get(ShardKey.fromShardId(shardId));
            return Objects.requireNonNullElse(shardInfo, ShardSample.EMPTY).shardInfo;
        }

        SampledClusterMetrics withStatus(Set<SamplingStatus> newStatus) {
            return new SampledClusterMetrics(searchTierMetrics, indexTierMetrics, shardSamples, newStatus);
        }

    }

    record SampledTierMetrics(long memorySize, Activity activity) {
        static final SampledTierMetrics EMPTY = new SampledTierMetrics(0, Activity.EMPTY);
    }

    enum PersistentTaskNodeStatus {
        NO_NODE,
        THIS_NODE,
        ANOTHER_NODE
    }

    final AtomicReference<SampledClusterMetrics> collectedMetrics = new AtomicReference<>(SampledClusterMetrics.EMPTY);
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
            collectedMetrics.set(SampledClusterMetrics.EMPTY);
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

        var collectSamplesRequest = new CollectClusterSamplesAction.Request(
            collectedMetrics.get().searchTierMetrics().activity(),
            collectedMetrics.get().indexTierMetrics().activity()
        );
        client.execute(CollectClusterSamplesAction.INSTANCE, collectSamplesRequest, new ActionListener<>() {
            @Override
            public void onResponse(CollectClusterSamplesAction.Response response) {
                Set<SamplingStatus> status = EnumSet.noneOf(SamplingStatus.class);
                if (response.isComplete() == false) {
                    collectionsPartialsCounter.increment();
                    status.add(SamplingStatus.PARTIAL);
                }

                // Update sample metrics, replacing memory, merging activity, and building new MeteringShardInfo from diffs
                collectedMetrics.getAndUpdate(
                    current -> new SampledClusterMetrics(
                        mergeTierMetrics(current.searchTierMetrics(), response.getSearchTierMemorySize(), response.getSearchActivity()),
                        mergeTierMetrics(current.indexTierMetrics(), response.getIndexTierMemorySize(), response.getIndexActivity()),
                        mergeShardInfos(removeStaleEntries(current.shardSamples()), response.getShardInfos()),
                        status
                    )
                );
                logger.debug(
                    () -> format(
                        "collected new metering shard info for shards [%s]",
                        response.getShardInfos().keySet().stream().map(ShardId::toString).collect(Collectors.joining(","))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                var previous = collectedMetrics.get();
                var status = EnumSet.copyOf(previous.status());
                status.add(SamplingStatus.STALE);
                collectedMetrics.set(previous.withStatus(status));
                logger.error("failed to collect metering shard info", e);
                collectionsErrorsCounter.increment();
            }
        });
    }

    private SampledTierMetrics mergeTierMetrics(SampledTierMetrics current, long newMemory, Activity newActivity) {
        return new SampledTierMetrics(newMemory, Activity.merge(Stream.of(current.activity(), newActivity), coolDown));
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

    public <T> Optional<T> withSamplesIfReady(Function<SampledClusterMetrics, T> function) {
        var currentInfo = collectedMetrics.get();
        if (persistentTaskNodeStatus != PersistentTaskNodeStatus.THIS_NODE || currentInfo == SampledClusterMetrics.EMPTY) {
            return Optional.empty(); // not the PersistentTask node or not ready to report yet
        }
        return Optional.ofNullable(function.apply(currentInfo));
    }

    public SampledMetricsProvider createSampledStorageMetricsProvider() {
        return new SampledStorageMetricsProvider(this, clusterService);
    }

    public SampledVCUMetricsProvider createSampledVCUMetricsProvider(NodeEnvironment nodeEnvironment) {
        var coolDownPeriod = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
        var spMinProvisionedMemoryProvider = SampledVCUMetricsProvider.SPMinProvisionedMemoryProvider.build(
            clusterService,
            nodeEnvironment
        );
        return new SampledVCUMetricsProvider(this, coolDownPeriod, spMinProvisionedMemoryProvider, meterRegistry);
    }

    private static Map<ShardKey, ShardSample> mergeShardInfos(Map<ShardKey, ShardSample> current, Map<ShardId, ShardInfoMetrics> updated) {
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
        return map;
    }

    public SampledShardInfos getMeteringShardInfo() {
        return collectedMetrics.get();
    }

    SampledTierMetrics getSearchTierMetrics() {
        var sampledClusterMetrics = collectedMetrics.get();
        assert sampledClusterMetrics != null;
        return sampledClusterMetrics.searchTierMetrics();
    }

    SampledTierMetrics getIndexTierMetrics() {
        var sampledClusterMetrics = collectedMetrics.get();
        assert sampledClusterMetrics != null;
        return sampledClusterMetrics.indexTierMetrics();
    }

    private LongWithAttributes makeActivityMeter(SampledTierMetrics metrics) {
        var now = Instant.now();
        var activity = metrics.activity();

        assert activity.lastActivityRecentPeriod().isBefore(now);
        var isActive = activity.isActive(now, coolDown);

        if (isActive) {
            long secondsActive = activity.firstActivityRecentPeriod().until(now, ChronoUnit.SECONDS);
            return new LongWithAttributes(secondsActive);
        } else if (activity.isEmpty()) {
            // Special case, inactive, but we don't know for how long
            return new LongWithAttributes(-1);
        } else {
            long secondsInactive = activity.lastActivityRecentPeriod().plus(coolDown).until(now, ChronoUnit.SECONDS);
            return new LongWithAttributes(-secondsInactive);
        }
    }
}
