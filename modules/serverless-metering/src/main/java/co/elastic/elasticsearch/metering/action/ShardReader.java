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

import co.elastic.elasticsearch.metering.MeteringShardInfoService;

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

public class ShardReader {
    private static final Logger logger = LogManager.getLogger(ShardReader.class);
    private final IndicesService indicesService;

    public ShardReader(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    private long computeShardSize(ShardId shardId, SegmentInfos segmentInfos) throws IOException {
        long size = 0;
        for (SegmentCommitInfo si : segmentInfos) {
            try {
                long commitSize = si.sizeInBytes();
                size += commitSize;
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
        return size;
    }

    public Map<ShardId, MeteringShardInfo> getShardSizes(MeteringShardInfoService meteringShardInfoService) throws IOException {
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

                var cachedShardInfo = meteringShardInfoService.getCachedShardInfo(shardId, primaryTerm, generation);
                var size = computeShardSize(shardId, segmentInfos);
                logger.debug(
                    "cached shard size for [{}] at [{}:{}] is [{}]",
                    shardId,
                    primaryTerm,
                    generation,
                    cachedShardInfo.map(x -> Long.toString(x.size())).orElse("<not present>")
                );
                // TODO (ES-7851): only insert diffs here (cachedShardSize.isEmpty() || cachedShardSize.get().size() != size)
                shardIds.put(shardId, new MeteringShardInfo(size, primaryTerm, generation));

                meteringShardInfoService.updateCachedShardInfo(shardId, primaryTerm, generation, size);
            }
        }
        return shardIds;
    }
}
