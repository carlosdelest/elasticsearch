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

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class LocalNodeMeteringShardInfoCache extends AbstractLifecycleComponent {
    record CacheEntry(long primaryTerm, long generation, long sizeInBytes, long docCount, String token) {}

    private Map<ShardId, CacheEntry> shardSizeCache = new ConcurrentHashMap<>();

    @Override
    protected void doStart() {
        shardSizeCache = new ConcurrentHashMap<>();
    }

    @Override
    protected void doStop() {
        shardSizeCache = new ConcurrentHashMap<>();
    }

    @Override
    protected void doClose() {}

    Optional<CacheEntry> getCachedShardInfo(ShardId shardId, long primaryTerm, long generation) {
        var cacheEntry = shardSizeCache.get(shardId);
        if (cacheEntry != null && cacheEntry.primaryTerm == primaryTerm && cacheEntry.generation == generation) {
            return Optional.of(cacheEntry);
        }
        return Optional.empty();
    }

    void updateCachedShardInfo(ShardId shardId, long primaryTerm, long generation, long sizeInBytes, long docCount, String token) {
        assert shardId != null;
        assert token != null;
        shardSizeCache.put(shardId, new CacheEntry(primaryTerm, generation, sizeInBytes, docCount, token));
    }
}
