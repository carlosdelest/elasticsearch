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

    private record ShardSizeAndDocCount(long sizeInBytes, long docCount) {}

    private ShardSizeAndDocCount computeShardStats(ShardId shardId, SegmentInfos segmentInfos) throws IOException {
        long sizeInBytes = 0;
        long docCount = 0;
        for (SegmentCommitInfo si : segmentInfos) {
            try {
                long commitSize = si.sizeInBytes();
                long commitDocCount = si.info.maxDoc() - si.getDelCount() - si.getSoftDelCount();
                sizeInBytes += commitSize;
                docCount += commitDocCount;
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
        return new ShardSizeAndDocCount(sizeInBytes, docCount);
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
                            shardSizeAndDocCount.docCount(),
                            primaryTerm,
                            generation,
                            null // TODO
                        )
                    );
                    if (requestCacheToken != null) {
                        localNodeMeteringShardInfoCache.updateCachedShardInfo(
                            shardId,
                            primaryTerm,
                            generation,
                            shardSizeAndDocCount.sizeInBytes(),
                            shardSizeAndDocCount.docCount(),
                            requestCacheToken,
                            null // TODO
                        );
                    }
                }
            }
        }
        return shardIds;
    }
}
