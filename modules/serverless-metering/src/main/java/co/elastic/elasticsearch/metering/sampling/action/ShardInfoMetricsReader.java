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

package co.elastic.elasticsearch.metering.sampling.action;

import co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsProvider;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

interface ShardInfoMetricsReader {
    Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken);

    class NoOpReader implements ShardInfoMetricsReader {
        @Override
        public Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken) {
            return Collections.emptyMap();
        }
    }

    class DefaultShardInfoMetricsReader implements ShardInfoMetricsReader {
        private static final Logger logger = LogManager.getLogger(ShardInfoMetricsReader.class);

        static final String SHARD_INFO_SHARDS_TOTAL_METRIC = "es.metering.shard_info.shards.total";
        static final String SHARD_INFO_CACHED_TOTAL_METRIC = "es.metering.shard_info.cached.total";
        static final String SHARD_INFO_UNAVAILABLE_TOTAL_METRIC = "es.metering.shard_info.unavailable.total";
        static final String SHARD_INFO_RA_STORAGE_NEWER_GEN_TOTAL_METRIC = "es.metering.shard_info.computed.total";
        static final String SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC = "es.metering.shard_info.rastorage.approximated.ratio";

        private final IndicesService indicesService;
        private final ShardSizeStatsProvider shardSizeStatsProvider;
        private final InMemoryShardInfoMetricsCache shardMetricsCache;

        private final LongCounter shardInfoShardsTotalCounter;
        private final LongCounter shardInfoCachedTotalCounter;
        private final LongCounter shardInfoUnavailableTotalCounter;
        private final LongCounter shardInfoRaStorageNewerGenTotalCounter;
        private final DoubleHistogram shardInfoRaStorageApproximatedRatio;

        DefaultShardInfoMetricsReader(
            IndicesService indicesService,
            ShardSizeStatsProvider shardSizeStatsProvider,
            MeterRegistry meterRegistry
        ) {
            this(indicesService, shardSizeStatsProvider, new InMemoryShardInfoMetricsCache(), meterRegistry);
        }

        DefaultShardInfoMetricsReader(
            IndicesService indicesService,
            ShardSizeStatsProvider shardSizeStatsProvider,
            InMemoryShardInfoMetricsCache shardMetricsCache,
            MeterRegistry meterRegistry
        ) {
            this.indicesService = indicesService;
            this.shardSizeStatsProvider = shardSizeStatsProvider;
            this.shardMetricsCache = shardMetricsCache;

            this.shardInfoShardsTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_SHARDS_TOTAL_METRIC,
                "Total number of shard infos processed",
                "unit"
            );
            this.shardInfoCachedTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_CACHED_TOTAL_METRIC,
                "Total number of shard infos resulting in a cache hit",
                "unit"
            );
            this.shardInfoUnavailableTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_UNAVAILABLE_TOTAL_METRIC,
                "Total number of shard infos skipped due to shard unavailability",
                "unit"
            );
            this.shardInfoRaStorageNewerGenTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_RA_STORAGE_NEWER_GEN_TOTAL_METRIC,
                "Total number of shard infos with RA-S on a newer generation",
                "unit"
            );
            this.shardInfoRaStorageApproximatedRatio = meterRegistry.registerDoubleHistogram(
                SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC,
                "Percentage of approximated segment sizes per shard",
                "unit"
            );
        }

        private static Long getRAStorageFromUserData(SegmentInfos segmentInfos, ShardId shardId) {
            var raStorageString = segmentInfos.getUserData().get(RAStorageAccumulator.RA_STORAGE_KEY);
            if (raStorageString == null) {
                return null;
            }
            long raStorage = Long.parseLong(raStorageString);
            if (raStorage < 0) {
                logger.warn("skipping negative RA-S in UserData [{}] for shard [{}]", raStorageString, shardId);
                return null;
            }

            logger.trace("using RA-S from UserData [{}] for shard [{}]", raStorageString, shardId);
            return raStorage;
        }

        private static Long getRAStorageFromSegmentAttribute(
            ShardId shardId,
            SegmentCommitInfo si,
            long commitLiveDocs,
            boolean isExact,
            List<Long> avgRASizeList
        ) {
            var avgRASizeAttribute = si.info.getAttribute(RAStorageAccumulator.RA_STORAGE_AVG_KEY);
            if (avgRASizeAttribute == null) {
                return null;
            }
            var avgRASize = Long.parseLong(avgRASizeAttribute);
            if (avgRASize < 0) {
                // Due to bug related to ES-8577, we recorded the default raw size (-1, meaning not metered) for documents
                // replayed from translog, potentially resulting into a negative RA-S avg per doc. We have to skip such
                // segments here to minimize the impact.
                logger.warn(
                    "skipping negative RA-S (avg: [{}], live docs: [{}]) for segment [{}/{}]",
                    avgRASize,
                    commitLiveDocs,
                    shardId,
                    si.info.name
                );
                return null;
            }

            avgRASizeList.add(avgRASize);
            var raStorage = avgRASize * commitLiveDocs;
            logger.trace(
                "using {} RA-S [{}] (avg: [{}], live docs: [{}]) for segment [{}/{}]",
                isExact ? "exact" : "approximated",
                raStorage,
                avgRASize,
                commitLiveDocs,
                shardId,
                si.info.name
            );
            return raStorage;
        }

        ShardInfoMetrics computeShardInfo(ShardId shardId, ShardSize shardSize, long indexCreationDate, SegmentInfos segmentInfos) {
            // TODO: Moving liveDocCount into ShardSize would allow to skip this entirely if a project doesn't track RA-S.
            long liveDocCount = 0;
            long raLiveDocCount = 0;
            long deletedDocCount = 0;
            long raDeletedDocCount = 0;
            long raApproximatedDocCount = 0;
            Long totalRAValue = null;
            int segmentCount = segmentInfos.size();
            int raSegmentCount = 0;
            int raApproximatedSegmentCount = 0;
            List<Long> avgRASizeList = new ArrayList<>();

            for (SegmentCommitInfo si : segmentInfos) {
                long commitTotalDocCount = si.info.maxDoc();
                long commitDeletedDocCount = si.getDelCount() + si.getSoftDelCount();
                long commitLiveDocCount = commitTotalDocCount - commitDeletedDocCount;

                liveDocCount += commitLiveDocCount;
                deletedDocCount += commitDeletedDocCount;

                boolean isExact = commitLiveDocCount == commitTotalDocCount;
                var raStorage = getRAStorageFromSegmentAttribute(shardId, si, commitLiveDocCount, isExact, avgRASizeList);
                if (raStorage != null) {
                    totalRAValue = raStorage + (totalRAValue != null ? totalRAValue : 0);
                    ++raSegmentCount;
                    raLiveDocCount += commitLiveDocCount;
                    raDeletedDocCount += commitDeletedDocCount;
                    if (isExact == false) {
                        ++raApproximatedSegmentCount;
                        raApproximatedDocCount += commitLiveDocCount;
                    }
                }
            }

            if (totalRAValue != null) {
                // report ratio of approximated segments per shard in histogram
                double approximatedSegmentsRatio = raSegmentCount == 0 ? 0 : (double) raApproximatedSegmentCount / raSegmentCount;
                this.shardInfoRaStorageApproximatedRatio.record(
                    approximatedSegmentsRatio,
                    Map.of("index", shardId.getIndexName(), "shard", Integer.toString(shardId.id()))
                );
            } else {
                // Try to use the per shard RA value (timeseries indices)
                totalRAValue = getRAStorageFromUserData(segmentInfos, shardId);
                if (totalRAValue == null) {
                    logger.trace("No RA-S available for shard [{}]", shardId);
                    totalRAValue = 0L;
                }
            }

            boolean hasRAStats = avgRASizeList.isEmpty() == false;
            final ShardInfoMetrics.RawStoredSizeStats raStats;
            if (hasRAStats == false) {
                raStats = ShardInfoMetrics.RawStoredSizeStats.EMPTY;
            } else {
                var avgRASizeStats = fillAvgRASizeStats(avgRASizeList);
                raStats = new ShardInfoMetrics.RawStoredSizeStats(
                    raSegmentCount,
                    raLiveDocCount,
                    raDeletedDocCount,
                    raApproximatedDocCount,
                    avgRASizeStats.min(),
                    avgRASizeStats.max(),
                    avgRASizeStats.total(),
                    avgRASizeStats.squaredTotal()
                );
            }

            return new ShardInfoMetrics(
                liveDocCount,
                shardSize.interactiveSizeInBytes(),
                shardSize.nonInteractiveSizeInBytes(),
                totalRAValue,
                shardSize.primaryTerm(),
                shardSize.generation(),
                indexCreationDate,
                segmentCount,
                deletedDocCount,
                raStats
            );
        }

        private record AvgRASizeStats(long min, long max, double total, double squaredTotal) {}

        private static AvgRASizeStats fillAvgRASizeStats(List<Long> avgRASizeList) {
            double total = 0.0;
            double squaredTotal = 0.0;
            long min = Long.MAX_VALUE;
            long max = 0;
            for (Long x : avgRASizeList) {
                total += x;
                squaredTotal += (x * x);
                min = Math.min(min, x);
                max = Math.max(max, x);
            }

            return new AvgRASizeStats(min, max, total, squaredTotal);
        }

        @Override
        public Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken) {
            assert requestCacheToken != null : "cacheToken required";
            Map<ShardId, ShardInfoMetrics> shardsWithNewInfo = new HashMap<>();
            Set<ShardId> activeShards = new HashSet<>();
            for (final IndexService indexService : indicesService) {
                for (final IndexShard shard : indexService) {
                    if (shard.isSystem()) {
                        continue;
                    }
                    shardInfoShardsTotalCounter.increment();

                    ShardId shardId = shard.shardId();
                    // get pre-calculated shard size provided by SearchShardSizeCollector
                    ShardSize shardSize = shardSizeStatsProvider.getShardSize(shardId);
                    if (shardSize == null || shard.getOperationPrimaryTerm() != shardSize.primaryTerm()) {
                        // The shard is currently not yet available or a new primary was promoted since gathering the latest shard sizes.
                        // From a metering perspective it seems ok to not provide an update for this shard at this point.
                        shardInfoUnavailableTotalCounter.increment();
                        continue;
                    }

                    activeShards.add(shardId);
                    // Caching is based on the latest published primary term and generation of SearchShardSizeCollector
                    // Note that the cached total RA-S value might be slightly ahead on a newer generation as it is calculated adhoc
                    // with a possible delay of up to the publishing frequency of SearchShardSizeCollector.
                    long primaryTerm = shardSize.primaryTerm();
                    long generation = shardSize.generation();
                    var cachedShardInfo = shardMetricsCache.getCachedShardMetrics(shardId, primaryTerm, generation);
                    if (cachedShardInfo.isPresent()) {
                        // Cached information is up-to-date
                        var shardInfo = cachedShardInfo.get().shardInfo();
                        var token = cachedShardInfo.get().token();
                        shardInfoCachedTotalCounter.increment();
                        logger.debug("cached shard info for [{}]: [{}]", shardId, shardInfo);

                        // If requester changed from the last time, include this shard info in the response and update the cache entry with
                        // the new request token
                        if (token.equals(requestCacheToken) == false) {
                            shardsWithNewInfo.put(shardId, shardInfo);
                            shardMetricsCache.updateCachedShardMetrics(shardId, requestCacheToken, shardInfo);
                        }

                    } else {
                        // Cached information is outdated or missing: re-compute shard stats, include in response, and update cache entry
                        Engine engine = shard.getEngineOrNull();
                        if (engine == null) {
                            // The shard just became unavailable (we got a valid, cached shard size).
                            // It's ok to not provide an update for this shard during this collection.
                            activeShards.remove(shardId);
                            shardInfoUnavailableTotalCounter.increment();
                            continue;
                        }
                        var segmentInfos = engine.getLastCommittedSegmentInfos();
                        // Total RA-S is an approximation, using a slightly newer generation than the pre-calculated shard size is ok.
                        // However, track this using an APM metric which is to be looked at relative to #shards - #unavailable - #cached
                        if (segmentInfos.getGeneration() != shardSize.generation()) {
                            shardInfoRaStorageNewerGenTotalCounter.increment();
                        }
                        var indexCreationDate = indexService.getMetadata().getCreationDate();
                        var shardInfo = computeShardInfo(shardId, shardSize, indexCreationDate, segmentInfos);
                        shardsWithNewInfo.put(shardId, shardInfo);
                        shardMetricsCache.updateCachedShardMetrics(shardId, requestCacheToken, shardInfo);
                    }
                }
            }
            shardMetricsCache.retainActive(activeShards);
            return shardsWithNewInfo;
        }
    }
}
