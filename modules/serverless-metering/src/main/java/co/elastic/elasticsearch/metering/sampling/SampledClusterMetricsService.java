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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public SampledClusterMetricsService(ClusterService clusterService, MeterRegistry meterRegistry) {
        this(clusterService, meterRegistry, PersistentTaskNodeStatus.NO_NODE);
    }

    @SuppressWarnings("this-escape")
    protected SampledClusterMetricsService(
        ClusterService clusterService,
        MeterRegistry meterRegistry,
        PersistentTaskNodeStatus nodeStatus
    ) {
        this.clusterService = clusterService;
        this.meterRegistry = meterRegistry;
        this.metricsState = new AtomicReference<>(new SamplingState(nodeStatus, SampledClusterMetrics.EMPTY));

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
            () -> this.withSamplesIfReady(sample -> makeActivityMeter(sample.searchTierMetrics()), null).stream().toList()
        );
        meterRegistry.registerLongsGauge(
            NODE_INFO_TIER_INDEX_ACTIVITY_TIME,
            "The seconds since index tier last became active or inactive. Value is positive if active, and negative if inactive",
            "sec",
            () -> this.withSamplesIfReady(sample -> makeActivityMeter(sample.indexTierMetrics()), null).stream().toList()
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

        SampledClusterMetrics withAdditionalStatus(SamplingStatus newStatus) {
            var newSet = EnumSet.copyOf(status);
            newSet.add(newStatus);
            return new SampledClusterMetrics(searchTierMetrics, indexTierMetrics, shardSamples, newSet);
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

    record SamplingState(PersistentTaskNodeStatus nodeStatus, SampledClusterMetrics metrics) {}

    final AtomicReference<SamplingState> metricsState;

    /**
     * Monitors cluster state changes to see if we are not the persistent task node anymore.
     * If we are not the persistent task node anymore, reset our cached collected shard info.
     * Package-private for testing.
     */
    void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return; // metadata (task assignment) unchanged
        }

        var previousTaskNodeId = SampledClusterMetricsSchedulingTask.findTaskNodeId(event.previousState());
        var currentTaskNodeId = SampledClusterMetricsSchedulingTask.findTaskNodeId(event.state());

        var localNodeId = event.state().nodes().getLocalNodeId();
        var isPersistentTask = localNodeId != null && localNodeId.equals(currentTaskNodeId);
        var wasPersistentTask = localNodeId != null && localNodeId.equals(previousTaskNodeId);

        if (isPersistentTask && wasPersistentTask == false) {
            // this node has become the persistent task node
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.THIS_NODE, SampledClusterMetrics.EMPTY));
        } else if (currentTaskNodeId == null && previousTaskNodeId != null) {
            // the persistent task node was unassigned (e.g. node shutdown) or the task was disabled
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.NO_NODE, SampledClusterMetrics.EMPTY));
        } else if (currentTaskNodeId != null && currentTaskNodeId.equals(previousTaskNodeId) == false) {
            // another node has become the persistent task node
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.ANOTHER_NODE, SampledClusterMetrics.EMPTY));
        }
    }

    /**
     * Updates the internal storage of metering shard info, by performing a scatter-gather operation towards all (search) nodes
     */
    void updateSamples(Client client) {
        logger.debug("Calling SampledClusterMetricsService#updateSamples");
        collectionsTotalCounter.increment();

        var state = this.metricsState.get();
        var collectSamplesRequest = new CollectClusterSamplesAction.Request(
            state.metrics().searchTierMetrics().activity(),
            state.metrics().indexTierMetrics().activity()
        );
        client.execute(CollectClusterSamplesAction.INSTANCE, collectSamplesRequest, new ActionListener<>() {
            @Override
            public void onResponse(CollectClusterSamplesAction.Response response) {
                if (response.isComplete() == false) {
                    collectionsPartialsCounter.increment();
                }
                // Update sample metrics, replacing memory, merging activity, and building new MeteringShardInfo from diffs
                SampledClusterMetricsService.this.metricsState.getAndUpdate(
                    current -> current.nodeStatus() == PersistentTaskNodeStatus.THIS_NODE
                        ? new SamplingState(current.nodeStatus(), mergeSamplesWithUpdate(current.metrics(), response))
                        : current
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
                logger.error("Failed to collect samples in cluster", e);
                collectionsErrorsCounter.increment();
                SampledClusterMetricsService.this.metricsState.getAndUpdate(
                    current -> current.nodeStatus() == PersistentTaskNodeStatus.THIS_NODE
                        ? new SamplingState(current.nodeStatus(), current.metrics().withAdditionalStatus(SamplingStatus.STALE))
                        : current
                );
            }
        });
    }

    private SampledClusterMetrics mergeSamplesWithUpdate(SampledClusterMetrics current, CollectClusterSamplesAction.Response response) {
        return new SampledClusterMetrics(
            mergeTierMetrics(current.searchTierMetrics(), response.getSearchTierMemorySize(), response.getSearchActivity()),
            mergeTierMetrics(current.indexTierMetrics(), response.getIndexTierMemorySize(), response.getIndexActivity()),
            mergeShardInfos(removeStaleEntries(current.shardSamples()), response.getShardInfos()),
            response.isComplete() ? EnumSet.noneOf(SamplingStatus.class) : EnumSet.of(SamplingStatus.PARTIAL)
        );
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

    public <T> Optional<T> withSamplesIfReady(
        Function<SampledClusterMetrics, T> samplesFunction,
        @Nullable Consumer<PersistentTaskNodeStatus> otherwise
    ) {
        var metricsState = this.metricsState.get();
        var sampledMetrics = metricsState.metrics();
        // sampledMetrics may only be non-empty if this is the persistent task node
        assert sampledMetrics == SampledClusterMetrics.EMPTY || metricsState.nodeStatus() == PersistentTaskNodeStatus.THIS_NODE;
        // if empty, not the persistent task node or not ready to report yet
        if (sampledMetrics != SampledClusterMetrics.EMPTY) {
            return Optional.ofNullable(samplesFunction.apply(sampledMetrics));
        }
        if (otherwise != null) {
            otherwise.accept(metricsState.nodeStatus());
        }
        return Optional.empty();
    }

    public SampledMetricsProvider createSampledStorageMetricsProvider(SystemIndices systemIndices) {
        return new SampledStorageMetricsProvider(this, clusterService, systemIndices);
    }

    public SampledVCUMetricsProvider createSampledVCUMetricsProvider(NodeEnvironment nodeEnvironment, SystemIndices systemIndices) {
        var coolDownPeriod = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
        var spMinProvisionedMemoryCalculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, nodeEnvironment);
        return new SampledVCUMetricsProvider(this, coolDownPeriod, spMinProvisionedMemoryCalculator, meterRegistry);
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
        return getSampledClusterMetrics();
    }

    private SampledClusterMetrics getSampledClusterMetrics() {
        var metricsState = this.metricsState.get();
        assert metricsState != null && metricsState.metrics() != null;
        return metricsState.metrics();
    }

    SampledTierMetrics getSearchTierMetrics() {
        return getSampledClusterMetrics().searchTierMetrics();
    }

    SampledTierMetrics getIndexTierMetrics() {
        return getSampledClusterMetrics().indexTierMetrics();
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
