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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.NodeSearchLoadSnapshot;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.PublishNodeSearchLoadRequest;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * This service is responsible for receiving raw shard sizes notification from the search nodes
 * and aggregating them into search tier autoscaling metrics.
 */
public class SearchMetricsService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchMetricsService.class);

    /**
     * Samples older than this value will not be considered exact.
     */
    public static final Setting<TimeValue> ACCURATE_METRICS_WINDOW_SETTING = Setting.timeSetting(
        "serverless.autoscaling.search_metrics.accurate_window",
        TimeValue.timeValueSeconds(90),
        TimeValue.timeValueSeconds(3),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> STALE_METRICS_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.autoscaling.search_metrics.stale_check_interval",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final ShardSize ZERO_SHARD_SIZE = new ShardSize(0, 0, PrimaryTermAndGeneration.ZERO);

    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;

    private volatile boolean shardMetricsInitialized = false;
    private volatile boolean loadMetricsInitialized = false;

    private final ConcurrentMap<Index, IndexProperties> indices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeTimingForShardMetrics> nodeTimingForShardMetrics = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, ShardMetrics> shardMetrics = new ConcurrentHashMap<>();
    private final Map<String, NodeSearchLoad> nodeSearchLoads = new ConcurrentHashMap<>();

    private volatile long lastStaleMetricsCheckTimeNs = Long.MIN_VALUE;
    private volatile long accurateMetricWindowNs;
    private volatile long staleMetricsCheckIntervalNs;

    public static SearchMetricsService create(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService,
        MemoryMetricsService memoryMetricsService
    ) {
        SearchMetricsService service = new SearchMetricsService(clusterSettings, threadPool::relativeTimeInNanos, memoryMetricsService);
        clusterService.addListener(service);
        return service;
    }

    SearchMetricsService(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MemoryMetricsService memoryMetricsService
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
        clusterSettings.initializeAndWatch(ACCURATE_METRICS_WINDOW_SETTING, value -> this.accurateMetricWindowNs = value.getNanos());
        clusterSettings.initializeAndWatch(
            STALE_METRICS_CHECK_INTERVAL_SETTING,
            value -> this.staleMetricsCheckIntervalNs = value.getNanos()
        );
    }

    void processShardSizesRequest(PublishShardSizesRequest request) {
        var currentTimestampNanos = relativeTimeInNanos();
        logger.debug("Received shard sizes {}", request.getShardSizes());
        var nodeMetrics = this.nodeTimingForShardMetrics.computeIfAbsent(request.getNodeId(), unused -> new NodeTimingForShardMetrics());
        nodeMetrics.update(currentTimestampNanos);
        for (var entry : request.getShardSizes().entrySet()) {
            shardMetrics.computeIfAbsent(entry.getKey(), unused -> new ShardMetrics()).update(nodeMetrics, entry.getValue());
        }
    }

    public void processSearchLoadRequest(PublishNodeSearchLoadRequest request) {
        var nodeSearchLoad = nodeSearchLoads.computeIfAbsent(request.getNodeId(), unused -> new NodeSearchLoad(request.getNodeId()));
        nodeSearchLoad.setLatestReadingTo(request.getSearchLoad(), request.getQuality(), request.getSeqNo());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            shardMetricsInitialized = false;
            loadMetricsInitialized = false;
            indices.clear();
            nodeTimingForShardMetrics.clear();
            shardMetrics.clear();
            nodeSearchLoads.clear();

        } else {
            if (event.metadataChanged() || shardMetricsInitialized == false) {
                var state = event.state();
                indices.keySet().removeIf(index -> state.metadata().hasIndex(index) == false || hasZeroReplicas(index, state));
                shardMetrics.keySet()
                    .removeIf(
                        shardId -> state.metadata().hasIndex(shardId.getIndex()) == false || hasZeroReplicas(shardId.getIndex(), state)
                    );

                Function<ShardId, ShardMetrics> newShardMetricFactory = shardMetricsInitialized
                    ? newIndexShardMetricFactory()
                    : shardId -> new ShardMetrics();

                SortedMap<String, IndexAbstraction> indicesLookup = state.metadata().getIndicesLookup();
                for (IndexMetadata metadata : state.metadata().indices().values()) {
                    int shardCopies = getNumberOfReplicas(metadata);
                    if (shardCopies == 0) {
                        continue;
                    }

                    for (int i = 0; i < metadata.getNumberOfShards(); i++) {
                        shardMetrics.computeIfAbsent(new ShardId(metadata.getIndex(), i), newShardMetricFactory);
                    }

                    IndexAbstraction indexAbstraction = indicesLookup.get(metadata.getIndex().getName());
                    assert indexAbstraction.getType() != IndexAbstraction.Type.ALIAS : "we shouldn't see aliases here";
                    long indexRecency = getRecency(metadata, indexAbstraction);
                    indices.put(
                        metadata.getIndex(),
                        new IndexProperties(
                            metadata.getIndex().getName(),
                            metadata.getNumberOfShards(),
                            shardCopies,
                            metadata.isSystem(),
                            metadata.getAutoExpandReplicas().enabled(),
                            indexAbstraction.getParentDataStream() != null,
                            indexRecency
                        )
                    );
                }
            }
            if (event.nodesChanged() || shardMetricsInitialized == false) {
                var searchNodes = new HashSet<String>();
                for (DiscoveryNode node : event.state().nodes()) {
                    if (node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                        searchNodes.add(node.getId());
                    }
                }
                nodeTimingForShardMetrics.keySet().retainAll(searchNodes);
                for (String searchNode : searchNodes) {
                    nodeTimingForShardMetrics.computeIfAbsent(searchNode, unused -> new NodeTimingForShardMetrics());
                }
            }
            shardMetricsInitialized = true;

            if (event.nodesDelta().masterNodeChanged() || loadMetricsInitialized == false) {
                for (DiscoveryNode node : event.state().nodes()) {
                    if (isSearchNode(node)) {
                        nodeSearchLoads.computeIfAbsent(node.getId(), unused -> new NodeSearchLoad(node.getId()));
                    }
                }
                loadMetricsInitialized = true;
            }
            if (event.nodesChanged()) {
                for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                    if (isSearchNode(node)) {
                        nodeSearchLoads.computeIfAbsent(node.getId(), unused -> new NodeSearchLoad(node.getId()));
                    }
                }
                for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                    if (isSearchNode(removedNode)) {
                        var removedNodeSearchLoad = nodeSearchLoads.get(removedNode.getId());
                        if (removedNodeSearchLoad != null) {
                            // We don’t know if the node was just disconnected, and it will come back eventually, or if it’s dead.
                            removedNodeSearchLoad.setQualityToMinimumAndLoadToZero();
                        }
                    }
                }
            }
        }
    }

    /**
     * Use the creation date of an index as its recency by default.
     * Check if this is a data stream write index, in this case use {@link Long#MAX_VALUE} so we always order
     * write indices first.
     */
    private static long getRecency(IndexMetadata metadata, IndexAbstraction indexAbstraction) {
        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        if (parentDataStream != null && metadata.getIndex().equals(parentDataStream.getWriteIndex())) {
            return Long.MAX_VALUE;
        }
        return metadata.getCreationDate();
    }

    /**
     * Creates a metric factory for a newly created index.
     * It is initialized with a fake node so that metric quality stays exact when new/empty index is created
     */
    private Function<ShardId, ShardMetrics> newIndexShardMetricFactory() {
        var nodeMetrics = new NodeTimingForShardMetrics();
        nodeMetrics.timestamp = relativeTimeInNanos();
        return shardId -> {
            var shardMetrics = new ShardMetrics();
            shardMetrics.sourceNode = nodeMetrics;
            shardMetrics.shardSize = ZERO_SHARD_SIZE;
            return shardMetrics;
        };
    }

    public SearchTierMetrics getSearchTierMetrics() {
        var currentTimestampNanos = relativeTimeInNanos();

        boolean shardCopiesExact = shardMetricsInitialized;
        int maxShardCopies = 0;
        boolean dataSizeExact = shardMetricsInitialized;
        long maxInteractiveDataSizeInBytes = 0;
        long totalInteractiveDataSizeInBytes = 0;
        long totalDataSizeInBytes = 0;
        boolean checkStaleMetrics = currentTimestampNanos - staleMetricsCheckIntervalNs > lastStaleMetricsCheckTimeNs;
        if (checkStaleMetrics) {
            lastStaleMetricsCheckTimeNs = relativeTimeInNanos();
        }

        for (var entry : indices.entrySet()) {
            var index = entry.getKey();
            var settings = entry.getValue();
            maxShardCopies = Math.max(maxShardCopies, settings.replicas);
            for (int i = 0; i < settings.shards; i++) {
                var shardId = new ShardId(index, i);
                var metrics = shardMetrics.get(shardId);
                assert metrics != null : "Metric should be initialized by this point";
                synchronized (metrics) {
                    boolean isOutdated = metrics.sourceNode != null && metrics.sourceNode.isOutdated(currentTimestampNanos);
                    if (checkStaleMetrics && isOutdated) {
                        logger.warn("Storage metrics are stale for shard: {}, {}", shardId, metrics);
                    }
                    dataSizeExact &= metrics.sourceNode != null && isOutdated == false;
                    maxInteractiveDataSizeInBytes = Math.max(maxInteractiveDataSizeInBytes, metrics.shardSize.interactiveSizeInBytes());
                    totalInteractiveDataSizeInBytes += metrics.shardSize.interactiveSizeInBytes();
                    totalDataSizeInBytes += metrics.shardSize.totalSizeInBytes();
                }
            }
        }

        var nodeLoadIterator = nodeSearchLoads.entrySet().iterator();
        var searchLoads = new ArrayList<NodeSearchLoadSnapshot>();
        while (nodeLoadIterator.hasNext()) {
            var nodeSearchStatsEntry = nodeLoadIterator.next();
            var value = nodeSearchStatsEntry.getValue();
            if (value.isStale()) {
                nodeLoadIterator.remove();
            } else {
                searchLoads.add(value.getSearchLoadSnapshot());
            }
        }

        return new SearchTierMetrics(
            memoryMetricsService.getMemoryMetrics(),
            new MaxShardCopies(maxShardCopies, shardCopiesExact ? MetricQuality.EXACT : MetricQuality.MINIMUM),
            new StorageMetrics(
                maxInteractiveDataSizeInBytes,
                totalInteractiveDataSizeInBytes,
                totalDataSizeInBytes,
                dataSizeExact ? MetricQuality.EXACT : MetricQuality.MINIMUM
            ),
            Collections.unmodifiableList(searchLoads)
        );
    }

    /**
     * Stores timeStamp information used to determine metric quality for ShardMetrics.
     */
    private class NodeTimingForShardMetrics {
        private long timestamp = relativeTimeInNanos() - accurateMetricWindowNs - 1;

        private synchronized void update(long currentTimestamp) {
            this.timestamp = currentTimestamp;
        }

        private synchronized boolean isOutdated(long currentTimestampNanos) {
            return currentTimestampNanos - timestamp >= accurateMetricWindowNs;
        }
    }

    static class ShardMetrics {
        private NodeTimingForShardMetrics sourceNode = null;
        ShardSize shardSize = ZERO_SHARD_SIZE;

        private synchronized void update(NodeTimingForShardMetrics sourceNode, ShardSize shardSize) {
            if (this.shardSize.primaryTermGeneration().compareTo(shardSize.primaryTermGeneration()) <= 0) {
                this.sourceNode = sourceNode;
                this.shardSize = shardSize;
            }
        }

        @Override
        public String toString() {
            return Strings.format("ShardMetrics{timestamp=%d, shardSize=%s}", sourceNode != null ? sourceNode.timestamp : 0, shardSize);
        }
    }

    class NodeSearchLoad {
        private static final Logger SEARCH_LOAD_LOGGER = LogManager.getLogger(NodeSearchLoad.class);

        private final String nodeId;
        private double searchLoad;
        private long latestSampleTimeInNanos;
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        NodeSearchLoad(String nodeId) {
            this.nodeId = nodeId;
        }

        synchronized void setLatestReadingTo(double searchLoad, MetricQuality quality, long metricSeqNo) {
            if (metricSeqNo > maxSeqNo) {
                this.searchLoad = searchLoad;
                this.quality = quality;
                this.latestSampleTimeInNanos = relativeTimeInNanos();
                this.maxSeqNo = metricSeqNo;
                SEARCH_LOAD_LOGGER.trace(
                    "Set the load reading for node [{}] to: searchLoad: [{}], isSearchLoadValid: [{}], quality: [{}]",
                    nodeId,
                    searchLoad,
                    quality
                );
            }
        }

        synchronized void setQualityToMinimumAndLoadToZero() {
            this.quality = MetricQuality.MINIMUM;
            this.searchLoad = 0.0;
        }

        synchronized NodeSearchLoadSnapshot getSearchLoadSnapshot() {
            long timeSinceLastSampleInNanos = timeSinceLastSampleInNanos();
            if (quality == MetricQuality.EXACT && timeSinceLastSampleInNanos >= accurateMetricWindowNs) {
                quality = MetricQuality.MINIMUM;
                SEARCH_LOAD_LOGGER.trace(
                    "Set the quality to MINIMUM for node: [{}] due to time since last sample: [{}]NS GT exceeds accurate "
                        + "metric window: [{}]NS",
                    nodeId,
                    timeSinceLastSampleInNanos,
                    accurateMetricWindowNs
                );
            }
            return new NodeSearchLoadSnapshot(nodeId, searchLoad, quality);
        }

        synchronized boolean isStale() {
            return timeSinceLastSampleInNanos() >= staleMetricsCheckIntervalNs;
        }

        private long timeSinceLastSampleInNanos() {
            return relativeTimeInNanos() - latestSampleTimeInNanos;
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    record IndexProperties(
        String name,
        int shards,
        int replicas,
        boolean isSystem,
        boolean isAutoExpandReplicas,
        boolean isDataStream,
        long recency
    ) {}

    private static boolean hasZeroReplicas(Index index, ClusterState state) {
        return getNumberOfReplicas(state.metadata().index(index)) == 0;
    }

    private static int getNumberOfReplicas(IndexMetadata metadata) {
        return metadata.getAutoExpandReplicas() != null && metadata.getAutoExpandReplicas().enabled()
            // For indices with auto-expand we expect that at least 1 replica can be allocated
            ? Math.max(metadata.getAutoExpandReplicas().minReplicas(), 1)
            : metadata.getNumberOfReplicas();
    }

    ConcurrentMap<Index, IndexProperties> getIndices() {
        return indices;
    }

    ConcurrentMap<String, NodeTimingForShardMetrics> getNodeTimingForShardMetrics() {
        return nodeTimingForShardMetrics;
    }

    ConcurrentMap<ShardId, ShardMetrics> getShardMetrics() {
        return shardMetrics;
    }

    private static boolean isSearchNode(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
    }
}
