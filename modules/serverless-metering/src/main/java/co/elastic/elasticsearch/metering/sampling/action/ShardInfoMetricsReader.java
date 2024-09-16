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
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.StringHelper;
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

interface ShardInfoMetricsReader {
    Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken) throws IOException;

    class NoOpReader implements ShardInfoMetricsReader {
        @Override
        public Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken) {
            return Collections.emptyMap();
        }
    }

    class DefaultShardInfoMetricsReader implements ShardInfoMetricsReader {
        private static final Logger logger = LogManager.getLogger(ShardInfoMetricsReader.class);

        static final String SHARD_INFO_REQUESTS_TOTAL_METRIC = "es.metering.shard_info.requests.total";
        static final String SHARD_INFO_CACHED_TOTAL_METRIC = "es.metering.shard_info.cached.total";
        static final String SHARD_INFO_ERRORS_TOTAL_METRIC = "es.metering.shard_info.error.total";
        static final String SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC = "es.metering.shard_info.rastorage.approximated.ratio";

        private final IndicesService indicesService;
        private final ShardSizeStatsReader shardSizeStatsReader;
        private final InMemoryShardInfoMetricsCache shardMetricsCache;

        private final LongCounter shardInfoRequestsTotalCounter;
        private final LongCounter shardInfoCachedTotalCounter;
        private final LongCounter shardInfoErrorsTotalCounter;
        private final DoubleHistogram shardInfoRaStorageApproximatedRatio;

        DefaultShardInfoMetricsReader(
            IndicesService indicesService,
            ShardSizeStatsReader shardSizeStatsReader,
            MeterRegistry meterRegistry
        ) {
            this(indicesService, shardSizeStatsReader, new InMemoryShardInfoMetricsCache(), meterRegistry);
        }

        DefaultShardInfoMetricsReader(
            IndicesService indicesService,
            ShardSizeStatsReader shardSizeStatsReader,
            InMemoryShardInfoMetricsCache shardMetricsCache,
            MeterRegistry meterRegistry
        ) {
            this.indicesService = indicesService;
            this.shardSizeStatsReader = shardSizeStatsReader;
            this.shardMetricsCache = shardMetricsCache;
            this.shardInfoRequestsTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_REQUESTS_TOTAL_METRIC,
                "Total number of shard info requests processed",
                "unit"
            );
            this.shardInfoCachedTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_CACHED_TOTAL_METRIC,
                "Total number of shard info requests resulting in a cache hit",
                "unit"
            );
            this.shardInfoErrorsTotalCounter = meterRegistry.registerLongCounter(
                SHARD_INFO_ERRORS_TOTAL_METRIC,
                "Total number of errors computing shard infos",
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
            if (raStorageString != null) {
                logger.trace("using _rastorage from UserData [{}] for {}", raStorageString, shardId);
                return Long.parseLong(raStorageString);
            }
            return null;
        }

        ShardInfoMetrics computeShardInfo(
            ShardId shardId,
            long primaryTerm,
            long generation,
            long indexCreationDate,
            SegmentInfos segmentInfos
        ) throws IOException {
            long sizeInBytes = 0;
            long liveDocCount = 0;

            Long totalRAValue = null;
            final int segmentsInShard = segmentInfos.size();
            int approximatedSegmentsInShard = 0;
            for (SegmentCommitInfo si : segmentInfos) {
                try {
                    long commitSize = si.sizeInBytes();
                    long commitTotalDocCount = si.info.maxDoc();
                    long commitLiveDocCount = commitTotalDocCount - si.getDelCount() - si.getSoftDelCount();
                    sizeInBytes += commitSize;
                    liveDocCount += commitLiveDocCount;

                    var avgRASizePerDocAttribute = si.info.getAttribute(RAStorageAccumulator.RA_STORAGE_AVG_KEY);
                    if (avgRASizePerDocAttribute != null) {
                        var avgRASizePerDoc = Long.parseLong(avgRASizePerDocAttribute);
                        if (avgRASizePerDoc < 0) {
                            // Due to bug related to ES-8577, we recorded the default raw size (-1, meaning not metered) for documents
                            // replayed
                            // from translog, potentially resulting into a negative RA-S avg per doc. We have to skip such segments here to
                            // minimize the impact.
                            logger.warn(
                                "skipping segment with negative RA-S avg [{}] (live docs: [{}]) for {}",
                                avgRASizePerDoc,
                                liveDocCount,
                                shardId
                            );
                            continue;
                        }

                        var raStorage = avgRASizePerDoc * commitLiveDocCount;
                        boolean isExact = commitLiveDocCount == commitTotalDocCount;
                        if (isExact) {
                            logger.trace(
                                "using exact _rastorage [{}] (avg: [{}], live docs: [{}]) for {}",
                                raStorage,
                                avgRASizePerDoc,
                                liveDocCount,
                                shardId
                            );
                        } else {
                            ++approximatedSegmentsInShard;
                            logger.trace(
                                "using approximated _rastorage [{}] (avg: [{}], live docs: [{}], total docs: [{}]) for {}",
                                raStorage,
                                avgRASizePerDoc,
                                liveDocCount,
                                commitTotalDocCount,
                                shardId
                            );
                        }
                        if (totalRAValue == null) {
                            totalRAValue = raStorage;
                        } else {
                            totalRAValue += raStorage;
                        }
                    }
                } catch (IOException err) {
                    shardInfoErrorsTotalCounter.increment();
                    logger.warn(
                        "Failed to read file size for shard: [{}], commitId: [{}], err: [{}]",
                        shardId,
                        StringHelper.idToString(si.getId()),
                        err
                    );
                    throw err;
                }
            }

            if (totalRAValue != null) {
                // We computed and used RA per-segment
                double approximatedSegmentsRatio = segmentsInShard == 0
                    ? 0
                    : (double) approximatedSegmentsInShard / (double) segmentsInShard;
                this.shardInfoRaStorageApproximatedRatio.record(
                    approximatedSegmentsRatio,
                    Map.of("index", shardId.getIndexName(), "shard", Integer.toString(shardId.id()))
                );
            } else {
                // Try to use the per shard RA value (timeseries indices)
                totalRAValue = getRAStorageFromUserData(segmentInfos, shardId);
                if (totalRAValue == null) {
                    logger.trace("No _rastorage available for {}", shardId);
                    totalRAValue = 0L;
                }
            }

            return new ShardInfoMetrics(sizeInBytes, liveDocCount, primaryTerm, generation, totalRAValue, indexCreationDate);
        }

        @Override
        public Map<ShardId, ShardInfoMetrics> getUpdatedShardInfos(String requestCacheToken) throws IOException {
            Map<ShardId, ShardInfoMetrics> shardsWithNewInfo = new HashMap<>();
            Set<ShardId> activeShards = new HashSet<>();
            for (final IndexService indexService : indicesService) {
                for (final IndexShard shard : indexService) {
                    Engine engine = shard.getEngineOrNull();
                    if (engine == null || shard.isSystem()) {
                        continue;
                    }
                    shardInfoRequestsTotalCounter.increment();
                    ShardId shardId = shard.shardId();
                    activeShards.add(shardId);

                    SegmentInfos segmentInfos = engine.getLastCommittedSegmentInfos();
                    long primaryTerm = shard.getOperationPrimaryTerm();
                    long generation = segmentInfos.getGeneration();

                    var cachedShardInfo = shardMetricsCache.getCachedShardMetrics(shardId, primaryTerm, generation);

                    if (cachedShardInfo.isPresent()) {
                        // Cached information is up-to-date
                        var shardInfo = cachedShardInfo.get().shardInfo();
                        var token = cachedShardInfo.get().token();
                        shardInfoCachedTotalCounter.increment();
                        logger.debug(
                            "cached shard size for [{}] at [{}:{}] is [{}]",
                            shardId,
                            primaryTerm,
                            generation,
                            shardInfo.sizeInBytes()
                        );

                        // If requester changed from the last time, include this shard info in the response and update the cache entry with
                        // the new request token
                        if (token.equals(requestCacheToken) == false) {
                            shardsWithNewInfo.put(shardId, shardInfo);
                            shardMetricsCache.updateCachedShardMetrics(shardId, requestCacheToken, shardInfo);
                        }

                    } else {
                        // Cached information is outdated or missing: re-compute shard stats, include in response, and update cache entry
                        var indexCreationDate = indexService.getMetadata().getCreationDate();
                        var shardInfo = computeShardInfo(shardId, primaryTerm, generation, indexCreationDate, segmentInfos);
                        shardsWithNewInfo.put(shardId, shardInfo);
                        if (requestCacheToken != null) {
                            shardMetricsCache.updateCachedShardMetrics(shardId, requestCacheToken, shardInfo);
                        }
                    }
                }
            }
            shardMetricsCache.retainActive(activeShards);
            return shardsWithNewInfo;
        }
    }
}
