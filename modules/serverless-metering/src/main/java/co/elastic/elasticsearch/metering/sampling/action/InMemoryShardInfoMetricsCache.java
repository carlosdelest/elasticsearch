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

import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class InMemoryShardInfoMetricsCache {

    record CacheEntry(String token, ShardInfoMetrics shardInfo) {}

    // package private for testing
    final Map<ShardId, CacheEntry> shardMetricsCache = new ConcurrentHashMap<>();

    Optional<CacheEntry> getCachedShardMetrics(ShardId shardId, long primaryTerm, long generation) {
        var cacheEntry = shardMetricsCache.get(shardId);
        if (cacheEntry != null && cacheEntry.shardInfo.primaryTerm() == primaryTerm && cacheEntry.shardInfo.generation() == generation) {
            return Optional.of(cacheEntry);
        }
        return Optional.empty();
    }

    void updateCachedShardMetrics(ShardId shardId, String token, ShardInfoMetrics shardInfo) {
        assert shardId != null;
        assert token != null;
        shardMetricsCache.put(shardId, new CacheEntry(token, shardInfo));
    }

    void retainActive(Set<ShardId> activeShards) {
        shardMetricsCache.keySet().retainAll(activeShards);
    }
}
