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
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
    private final SystemIndices systemIndices;
    private final Duration activityCoolDownPeriod;
    private final MeterRegistry meterRegistry;
    private final LongCounter collectionsTotalCounter;
    private final LongCounter collectionsErrorsCounter;
    private final LongCounter collectionsPartialsCounter;

    public SampledClusterMetricsService(ClusterService clusterService, SystemIndices systemIndices, MeterRegistry meterRegistry) {
        this(clusterService, systemIndices, meterRegistry, PersistentTaskNodeStatus.NO_NODE);
    }

    @SuppressWarnings("this-escape")
    protected SampledClusterMetricsService(
        ClusterService clusterService,
        SystemIndices systemIndices,
        MeterRegistry meterRegistry,
        PersistentTaskNodeStatus nodeStatus
    ) {
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
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
        SampledStorageMetrics storageMetrics,
        Set<SamplingStatus> status
    ) implements SampledShardInfos {

        static final SampledClusterMetrics EMPTY = new SampledClusterMetrics(
            SampledTierMetrics.EMPTY,
            SampledTierMetrics.EMPTY,
            SampledStorageMetrics.EMPTY,
            Set.of(SamplingStatus.STALE)
        );

        public ShardInfoMetrics get(ShardId shardId) {
            var shardInfo = storageMetrics.shardInfos.get(shardId);
            return Objects.requireNonNullElse(shardInfo, ShardInfoMetrics.EMPTY);
        }

        SampledClusterMetrics withAdditionalStatus(SamplingStatus newStatus) {
            var newSet = EnumSet.copyOf(status);
            newSet.add(newStatus);
            return new SampledClusterMetrics(searchTierMetrics, indexTierMetrics, storageMetrics, newSet);
        }
    }

    static class SampledStorageMetrics {
        static final SampledStorageMetrics EMPTY = new SampledStorageMetrics(Collections.emptyMap(), Collections.emptyMap());

        private final Map<Index, Map<String, String>> indexMetadata;
        private final Map<ShardId, ShardInfoMetrics> shardInfos;

        // Index infos are accessed only sporadically when publishing usage reports (every 5 mins), but updated much more frequently
        // every time samples are collected (every 30 secs) and lazily initialized for that reason.
        // This is only ever accessed by the usage report collector thread, there's no concurrent access.
        private Map<Index, IndexInfoMetrics> indexInfos;

        SampledStorageMetrics(Map<ShardId, ShardInfoMetrics> shardInfos, Map<Index, Map<String, String>> indexMetadata) {
            this.shardInfos = shardInfos;
            this.indexMetadata = indexMetadata;
        }

        public Map<ShardId, ShardInfoMetrics> getShardInfos() {
            return shardInfos;
        }

        public Map<Index, IndexInfoMetrics> getIndexInfos() {
            var metrics = indexInfos;
            if (metrics == null) {
                indexInfos = metrics = IndexInfoMetrics.calculateIndexSamples(shardInfos, indexMetadata);
            }
            return metrics;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SampledStorageMetrics that = (SampledStorageMetrics) o;
            return Objects.equals(shardInfos, that.shardInfos) && Objects.equals(indexMetadata, that.indexMetadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardInfos, indexMetadata);
        }
    }

    record SampledTierMetrics(long memorySize, Activity activity) {
        static final SampledTierMetrics EMPTY = new SampledTierMetrics(0, Activity.EMPTY);

        SampledTierMetrics merge(long newMemory, Activity newActivity, Duration coolDown) {
            return new SampledTierMetrics(
                newMemory > 0 ? newMemory : memorySize,
                Activity.Merger.merge(Stream.of(activity(), newActivity), coolDown)
            );
        }
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
    void updateSamples(Client client, ActionListener<Void> listener) {
        logger.debug("Calling SampledClusterMetricsService#updateSamples");

        var state = metricsState.get();
        var collectSamplesRequest = new CollectClusterSamplesAction.Request(
            state.metrics().searchTierMetrics().activity(),
            state.metrics().indexTierMetrics().activity()
        );
        client.execute(CollectClusterSamplesAction.INSTANCE, collectSamplesRequest, new ActionListener<>() {
            @Override
            public void onResponse(CollectClusterSamplesAction.Response response) {
                collectionsTotalCounter.increment();
                if (response.isPartialSuccess()) {
                    collectionsPartialsCounter.increment();
                }

                try {
                    var clusterState = clusterService.state();
                    // Update sample metrics, replacing memory, merging activity, and building new MeteringShardInfo from diffs
                    metricsState.getAndUpdate(current -> nextSamplingState(current, clusterState, response));
                    logger.debug(
                        () -> format(
                            "collected new metering shard info for shards [%s]",
                            response.getShardInfos().keySet().stream().map(ShardId::toString).collect(Collectors.joining(","))
                        )
                    );
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to collect samples in cluster", e);
                collectionsTotalCounter.increment();
                collectionsErrorsCounter.increment();
                metricsState.getAndUpdate(
                    current -> current.nodeStatus() == PersistentTaskNodeStatus.THIS_NODE
                        ? new SamplingState(
                            current.nodeStatus(),
                            current.metrics().withAdditionalStatus(SamplingStatus.STALE),
                            current.committedTimestamp()
                        )
                        : current
                );
                listener.onFailure(e);
            }
        });
    }

    private SamplingState nextSamplingState(SamplingState current, ClusterState clusterState, CollectClusterSamplesAction.Response update) {
        if (current.nodeStatus() != PersistentTaskNodeStatus.THIS_NODE) {
            return current;
        }
        // if the committed timestamp proceeded, only retain active, known shards / index metadata and evict entries that have been removed
        var samplingMetadata = SampledMetricsMetadata.getFromClusterState(clusterState);
        var committedTimestamp = samplingMetadata != null ? samplingMetadata.getCommittedTimestamp() : Instant.EPOCH;
        var evictOldEntries = samplingMetadata == null || current.committedTimestamp.equals(committedTimestamp) == false;

        return new SamplingState(
            current.nodeStatus(),
            mergeSamplesWithUpdate(current.metrics(), update, evictOldEntries),
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
        boolean evictOldEntries
    ) {
        return new SampledClusterMetrics(
            current.searchTierMetrics.merge(
                response.getExtrapolatedSearchTierMemorySize(),
                response.getSearchActivity(),
                activityCoolDownPeriod
            ),
            current.indexTierMetrics.merge(
                response.getExtrapolatedIndexTierMemorySize(),
                response.getIndexActivity(),
                activityCoolDownPeriod
            ),
            mergeStorageMetrics(current.storageMetrics(), response.getShardInfos(), evictOldEntries),
            response.isPartialSuccess() ? EnumSet.of(SamplingStatus.PARTIAL) : EnumSet.noneOf(SamplingStatus.class)
        );
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
            new IndexSizeMetricsProvider(this, spMinProvisionedMemoryCalculator),
            new RawStorageMetricsProvider(this),
            new SampledVCUMetricsProvider(this, spMinProvisionedMemoryCalculator, meterRegistry)
        );
    }

    private SampledStorageMetrics mergeStorageMetrics(
        SampledStorageMetrics current,
        Map<ShardId, ShardInfoMetrics> updated,
        boolean evictOldEntries
    ) {
        final var state = clusterService.state();

        final var shardMetrics = new HashMap<>(current.shardInfos);
        final var indexMetadata = new HashMap<>(current.indexMetadata);

        // merge updates into shardInfos and initialize index metadata if missing
        final var indicesLookup = state.metadata().getDefaultProject().getIndicesLookup();
        for (var newEntry : updated.entrySet()) {
            indexMetadata.computeIfAbsent(newEntry.getKey().getIndex(), index -> initIndexMetadata(index, indicesLookup));
            shardMetrics.merge(newEntry.getKey(), newEntry.getValue(), (oldVal, newVal) -> newVal.mostRecent(oldVal));
        }

        if (evictOldEntries) {
            // check the routing table for known shards and evict outdated shard infos / index metadata
            var knownShardIds = new HashSet<ShardId>();
            var knownIndices = new HashSet<Index>();
            // TODO for multi-project sampled metrics must be maintained per project id
            for (var routing : state.globalRoutingTable().routingTable(Metadata.DEFAULT_PROJECT_ID)) {
                knownIndices.add(routing.getIndex());
                routing.allShards().forEach(shard -> knownShardIds.add(shard.shardId()));
            }
            shardMetrics.keySet().retainAll(knownShardIds);
            indexMetadata.keySet().retainAll(knownIndices);
        }
        return new SampledStorageMetrics(shardMetrics, indexMetadata);
    }

    private Map<String, String> initIndexMetadata(Index index, Map<String, IndexAbstraction> indicesLookup) {
        var indexAbstraction = indicesLookup.get(index.getName());
        return SourceMetadata.indexSourceMetadata(index, indexAbstraction, systemIndices);
    }

    public SampledShardInfos getMeteringShardInfo() {
        return getSampledClusterMetrics();
    }

    private SampledClusterMetrics getSampledClusterMetrics() {
        var metricsState = this.metricsState.get();
        assert metricsState != null && metricsState.metrics() != null;
        return metricsState.metrics();
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
