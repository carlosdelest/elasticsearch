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

import co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageAccumulator;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ShardReader {
    private static final Logger logger = LogManager.getLogger(ShardReader.class);
    private final IndicesService indicesService;

    ShardReader(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    record ShardSizeAndDocCount(long sizeInBytes, long liveDocCount, Long raSizeInBytes) {}

    private static Long computeApproximatedRAStorage(long avgRASizePerDoc, long liveDocCount, long totalDocCount, ShardId shardId) {
        var raStorage = avgRASizePerDoc * liveDocCount;
        if (liveDocCount == totalDocCount) {
            logger.trace(
                "using exact _rastorage [{}] (avg: [{}], live docs: [{}]) for {}",
                raStorage,
                avgRASizePerDoc,
                liveDocCount,
                shardId
            );
        } else {
            logger.trace(
                "using approximated _rastorage [{}] (avg: [{}], live docs: [{}], total docs: [{}]) for {}",
                raStorage,
                avgRASizePerDoc,
                liveDocCount,
                totalDocCount,
                shardId
            );
        }
        return raStorage;
    }

    private static Long getRAStorageFromUserData(SegmentInfos segmentInfos, ShardId shardId) {
        var raStorageString = segmentInfos.getUserData().get(RAStorageAccumulator.RA_STORAGE_KEY);
        if (raStorageString != null) {
            logger.trace("using _rastorage from UserData [{}] for {}", raStorageString, shardId);
            return Long.parseLong(raStorageString);
        }
        return null;
    }

    static ShardSizeAndDocCount computeShardStats(ShardId shardId, SegmentInfos segmentInfos) throws IOException {
        long sizeInBytes = 0;
        long liveDocCount = 0;

        Long totalRAValue = null;
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
                    if (totalRAValue == null) {
                        totalRAValue = computeApproximatedRAStorage(avgRASizePerDoc, commitLiveDocCount, commitTotalDocCount, shardId);
                    } else {
                        totalRAValue += computeApproximatedRAStorage(avgRASizePerDoc, commitLiveDocCount, commitTotalDocCount, shardId);
                    }
                }

            } catch (IOException err) {
                logger.warn(
                    "Failed to read file size for shard: [{}], commitId: [{}], err: [{}]",
                    shardId,
                    StringHelper.idToString(si.getId()),
                    err
                );
                throw err;
            }
        }

        if (totalRAValue == null) {
            // Try to use the per shard RA value (timeseries indices)
            totalRAValue = getRAStorageFromUserData(segmentInfos, shardId);
        }
        if (totalRAValue == null) {
            logger.trace("No _rastorage available for {}", shardId);
        }

        return new ShardSizeAndDocCount(sizeInBytes, liveDocCount, totalRAValue);
    }

    Map<ShardId, MeteringShardInfo> getMeteringShardInfoMap(
        LocalNodeMeteringShardInfoCache localNodeMeteringShardInfoCache,
        String requestCacheToken
    ) throws IOException {
        Map<ShardId, MeteringShardInfo> shardIds = new HashMap<>();
        for (final IndexService indexService : indicesService) {
            for (final IndexShard shard : indexService) {

                Engine engine = shard.getEngineOrNull();
                if (engine == null || shard.isSystem()) {
                    continue;
                }

                SegmentInfos segmentInfos = engine.getLastCommittedSegmentInfos();

                ShardId shardId = shard.shardId();
                long primaryTerm = shard.getOperationPrimaryTerm();
                long generation = segmentInfos.getGeneration();

                var cachedShardInfo = localNodeMeteringShardInfoCache.getCachedShardInfo(shardId, primaryTerm, generation);

                if (cachedShardInfo.isPresent()) {
                    // Cached information is up-to-date
                    logger.debug(
                        "cached shard size for [{}] at [{}:{}] is [{}]",
                        shardId,
                        primaryTerm,
                        generation,
                        Long.toString(cachedShardInfo.get().sizeInBytes())
                    );

                    // If requester changed from the last time, include this shard info in the response and update the cache entry with
                    // the new request token
                    if (cachedShardInfo.get().token().equals(requestCacheToken) == false) {
                        shardIds.put(
                            shardId,
                            new MeteringShardInfo(
                                cachedShardInfo.get().sizeInBytes(),
                                cachedShardInfo.get().docCount(),
                                primaryTerm,
                                generation,
                                cachedShardInfo.get().storedIngestSizeInBytes()
                            )
                        );
                        localNodeMeteringShardInfoCache.updateCachedShardInfo(
                            shardId,
                            primaryTerm,
                            generation,
                            cachedShardInfo.get().sizeInBytes(),
                            cachedShardInfo.get().docCount(),
                            requestCacheToken,
                            cachedShardInfo.get().storedIngestSizeInBytes()
                        );
                    }
                } else {
                    // Cached information is outdated or missing: re-compute shard stats, include in response, and update cache entry
                    var shardSizeAndDocCount = computeShardStats(shardId, segmentInfos);
                    shardIds.put(
                        shardId,
                        new MeteringShardInfo(
                            shardSizeAndDocCount.sizeInBytes(),
                            shardSizeAndDocCount.liveDocCount(),
                            primaryTerm,
                            generation,
                            shardSizeAndDocCount.raSizeInBytes()
                        )
                    );
                    if (requestCacheToken != null) {
                        localNodeMeteringShardInfoCache.updateCachedShardInfo(
                            shardId,
                            primaryTerm,
                            generation,
                            shardSizeAndDocCount.sizeInBytes(),
                            shardSizeAndDocCount.liveDocCount(),
                            requestCacheToken,
                            shardSizeAndDocCount.raSizeInBytes()
                        );
                    }
                }
            }
        }
        return shardIds;
    }

}
