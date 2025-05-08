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
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
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
    private final Duration activityCoolDownPeriod;
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
        this.metricsState = new AtomicReference<>(new SamplingState(nodeStatus, SampledClusterMetrics.EMPTY, Instant.EPOCH));

        clusterService.addListener(this::clusterChanged);
        this.activityCoolDownPeriod = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());

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

    record SampledClusterMetrics(
        SampledTierMetrics searchTierMetrics,
        SampledTierMetrics indexTierMetrics,
        Map<ShardId, ShardInfoMetrics> shardSamples,
        Set<SamplingStatus> status
    ) implements SampledShardInfos {

        static final SampledClusterMetrics EMPTY = new SampledClusterMetrics(
            SampledTierMetrics.EMPTY,
            SampledTierMetrics.EMPTY,
            Map.of(),
            Set.of(SamplingStatus.STALE)
        );

        public ShardInfoMetrics get(ShardId shardId) {
            var shardInfo = shardSamples.get(shardId);
            return Objects.requireNonNullElse(shardInfo, ShardInfoMetrics.EMPTY);
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

    record SamplingState(PersistentTaskNodeStatus nodeStatus, SampledClusterMetrics metrics, Instant committedTimestamp) {}

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
            var samplingMetadata = SampledMetricsMetadata.getFromClusterState(event.state());
            var committedTimestamp = samplingMetadata != null ? samplingMetadata.getCommittedTimestamp() : Instant.EPOCH;
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.THIS_NODE, SampledClusterMetrics.EMPTY, committedTimestamp));
        } else if (currentTaskNodeId == null && previousTaskNodeId != null) {
            // the persistent task node was unassigned (e.g. node shutdown) or the task was disabled
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.NO_NODE, SampledClusterMetrics.EMPTY, Instant.EPOCH));
        } else if (currentTaskNodeId != null && currentTaskNodeId.equals(previousTaskNodeId) == false) {
            // another node has become the persistent task node
            metricsState.set(new SamplingState(PersistentTaskNodeStatus.ANOTHER_NODE, SampledClusterMetrics.EMPTY, Instant.EPOCH));
        }
    }

    /**
     * Updates the internal storage of metering shard info, by performing a scatter-gather operation towards all (search) nodes
     */
    void updateSamples(Client client) {
        logger.debug("Calling SampledClusterMetricsService#updateSamples");
        collectionsTotalCounter.increment();

        var state = metricsState.get();
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

                var clusterState = clusterService.state();
                // Update sample metrics, replacing memory, merging activity, and building new MeteringShardInfo from diffs
                metricsState.getAndUpdate(current -> nextSamplingState(current, clusterState, response));
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
                metricsState.getAndUpdate(
                    current -> current.nodeStatus() == PersistentTaskNodeStatus.THIS_NODE
                        ? new SamplingState(
                            current.nodeStatus(),
                            current.metrics().withAdditionalStatus(SamplingStatus.STALE),
                            Instant.EPOCH
                        )
                        : current
                );
            }
        });
    }

    private SamplingState nextSamplingState(SamplingState current, ClusterState clusterState, CollectClusterSamplesAction.Response update) {
        if (current.nodeStatus() != PersistentTaskNodeStatus.THIS_NODE) {
            return current;
        }
        // if the committed timestamp proceeded, only retain active, known shards and evict shards that have been removed
        var samplingMetadata = SampledMetricsMetadata.getFromClusterState(clusterState);
        var committedTimestamp = samplingMetadata != null ? samplingMetadata.getCommittedTimestamp() : Instant.EPOCH;

        // check if to evict old shards (once the committed timestamp changed)
        var evictOldShards = samplingMetadata == null || current.committedTimestamp.equals(committedTimestamp) == false;

        return new SamplingState(
            current.nodeStatus(),
            mergeSamplesWithUpdate(current.metrics(), update, evictOldShards),
            committedTimestamp
        );
    }

    Activity.Snapshot activitySnapshot(SampledTierMetrics tierMetrics) {
        return tierMetrics.activity().activitySnapshot(Instant.now(), activityCoolDownPeriod);
    }

    Duration activityCoolDownPeriod() {
        return activityCoolDownPeriod;
    }

    private SampledClusterMetrics mergeSamplesWithUpdate(
        SampledClusterMetrics current,
        CollectClusterSamplesAction.Response response,
        boolean evictOldShards
    ) {
        return new SampledClusterMetrics(
            mergeTierMetrics(current.searchTierMetrics(), response.getSearchTierMemorySize(), response.getSearchActivity()),
            mergeTierMetrics(current.indexTierMetrics(), response.getIndexTierMemorySize(), response.getIndexActivity()),
            mergeShardInfos(current.shardSamples(), response.getShardInfos(), evictOldShards),
            response.isComplete() ? EnumSet.noneOf(SamplingStatus.class) : EnumSet.of(SamplingStatus.PARTIAL)
        );
    }

    private SampledTierMetrics mergeTierMetrics(SampledTierMetrics current, long newMemory, Activity newActivity) {
        return new SampledTierMetrics(newMemory, Activity.merge(Stream.of(current.activity(), newActivity), activityCoolDownPeriod));
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

    public Collection<SampledMetricsProvider> createSampledMetricsProviders(NodeEnvironment nodeEnvironment, SystemIndices systemIndices) {
        var spMinProvisionedMemoryCalculator = SPMinProvisionedMemoryCalculator.build(clusterService, systemIndices, nodeEnvironment);
        return List.of(
            new IndexSizeMetricsProvider(this, spMinProvisionedMemoryCalculator, clusterService, systemIndices),
            new RawStorageMetricsProvider(this, clusterService, systemIndices),
            new SampledVCUMetricsProvider(this, spMinProvisionedMemoryCalculator, meterRegistry)
        );
    }

    private Set<ShardId> activeShardIds() {
        return clusterService.state()
            .globalRoutingTable()
            .routingTables()
            .values()
            .stream()
            .flatMap(RoutingTable::allShards)
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
    }

    private Map<ShardId, ShardInfoMetrics> mergeShardInfos(
        Map<ShardId, ShardInfoMetrics> current,
        Map<ShardId, ShardInfoMetrics> updated,
        boolean evictOldShards
    ) {
        HashMap<ShardId, ShardInfoMetrics> map = new HashMap<>(current);
        for (var newEntry : updated.entrySet()) {
            // merge shard updates into map
            map.merge(newEntry.getKey(), newEntry.getValue(), (oldVal, newVal) -> newVal.mostRecent(oldVal));
        }
        if (evictOldShards) {
            // periodically we need to evict obsolete old shards
            map.keySet().retainAll(activeShardIds());
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

        if (activity.isBeforeLastCoolDownExpires(now, activityCoolDownPeriod)) {
            long secondsActive = activity.firstActivityRecentPeriod().until(now, ChronoUnit.SECONDS);
            return new LongWithAttributes(secondsActive);
        } else if (activity.isEmpty()) {
            // Special case, inactive, but we don't know for how long
            return new LongWithAttributes(-1);
        } else {
            long secondsInactive = activity.lastActivityRecentPeriod().plus(activityCoolDownPeriod).until(now, ChronoUnit.SECONDS);
            return new LongWithAttributes(-secondsInactive);
        }
    }
}
