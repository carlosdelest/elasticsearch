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
import co.elastic.elasticsearch.metering.sampling.action.InMemoryShardInfoMetricsCache.CacheEntry;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;

public class LocalNodeShardMetricsCacheTests extends ESTestCase {

    public void testSizeIsCached() {
        var shardId1 = new ShardId("index1", "index1UUID", 1);
        var shardId2 = new ShardId("index1", "index1UUID", 2);

        final String testNodeToken = "TEST-NODE";

        try (var shardSizeService = new InMemoryShardInfoMetricsCache()) {
            var shard1QueryResult = shardSizeService.getCachedShardMetrics(shardId1, 1, 1);

            var shard1Info2 = new ShardInfoMetrics(10L, 100L, 1, 1, 11L, 0);
            shardSizeService.updateCachedShardMetrics(shardId1, testNodeToken, shard1Info2);
            var secondShard1QueryResult = shardSizeService.getCachedShardMetrics(shardId1, 1, 1);

            var shard1Info3 = new ShardInfoMetrics(11L, 110L, 1, 1, 12L, 0);
            shardSizeService.updateCachedShardMetrics(shardId1, testNodeToken, shard1Info3);
            var thirdShard1QueryResult = shardSizeService.getCachedShardMetrics(shardId1, 1, 1);

            var shard2QueryResult = shardSizeService.getCachedShardMetrics(shardId2, 1, 1);

            var shard2Info2 = new ShardInfoMetrics(20L, 200L, 1, 1, 21L, 0);
            shardSizeService.updateCachedShardMetrics(shardId2, testNodeToken, shard2Info2);
            var secondShard2QueryResult = shardSizeService.getCachedShardMetrics(shardId2, 1, 1);

            var shard2Info3 = new ShardInfoMetrics(21L, 210L, 1, 1, 22L, 0);
            shardSizeService.updateCachedShardMetrics(shardId2, testNodeToken, shard2Info3);
            var thirdShard2QueryResult = shardSizeService.getCachedShardMetrics(shardId2, 1, 1);

            assertThat(shard1QueryResult, isEmpty());
            assertThat(secondShard1QueryResult, isPresentWith(new CacheEntry(testNodeToken, shard1Info2)));
            assertThat(thirdShard1QueryResult, isPresentWith(new CacheEntry(testNodeToken, shard1Info3)));

            assertThat(shard2QueryResult, isEmpty());
            assertThat(secondShard2QueryResult, isPresentWith(new CacheEntry(testNodeToken, shard2Info2)));
            assertThat(thirdShard2QueryResult, isPresentWith(new CacheEntry(testNodeToken, shard2Info3)));
        }
    }

    public void testCachedSizeNonMatchingWithDifferentPrimaryTermOrGeneration() {
        var shardId = new ShardId("index1", "index1UUID", 1);

        final String testNodeToken = "TEST-NODE";

        try (var shardSizeService = new InMemoryShardInfoMetricsCache()) {
            var firstQueryResult = shardSizeService.getCachedShardMetrics(shardId, 1, 1);

            var shardInfo = new ShardInfoMetrics(10L, 100L, 1, 1, 20L, 0);
            shardSizeService.updateCachedShardMetrics(shardId, testNodeToken, shardInfo);
            var secondQueryResult = shardSizeService.getCachedShardMetrics(shardId, 1, 1);

            var differentPrimaryQueryResult = shardSizeService.getCachedShardMetrics(shardId, 2, 1);
            var differentGenerationQueryResult = shardSizeService.getCachedShardMetrics(shardId, 1, 2);

            var updatedShardInfo = new ShardInfoMetrics(11L, 110L, 2, 2, 21L, 0);
            shardSizeService.updateCachedShardMetrics(shardId, testNodeToken, updatedShardInfo);
            var updatedQueryResult = shardSizeService.getCachedShardMetrics(shardId, 2, 2);

            assertThat(firstQueryResult, isEmpty());
            assertThat(secondQueryResult, isPresentWith(new CacheEntry(testNodeToken, shardInfo)));
            assertThat(differentPrimaryQueryResult, isEmpty());
            assertThat(differentGenerationQueryResult, isEmpty());
            assertThat(updatedQueryResult, isPresentWith(new CacheEntry(testNodeToken, updatedShardInfo)));
        }
    }

    public void testCachedSizeMatchingWithDifferentToken() {
        var shardId = new ShardId("index1", "index1UUID", 1);

        final String testNodeToken1 = "TEST-NODE1";
        final String testNodeToken2 = "TEST-NODE2";

        try (var shardSizeService = new InMemoryShardInfoMetricsCache()) {
            var shardInfo = new ShardInfoMetrics(10L, 100L, 1, 1, 0, 0);
            shardSizeService.updateCachedShardMetrics(shardId, testNodeToken1, shardInfo);
            var queryResult = shardSizeService.getCachedShardMetrics(shardId, 1, 1);

            var updatedShardInfo = new ShardInfoMetrics(11L, 110L, 2, 2, 0, 0);
            shardSizeService.updateCachedShardMetrics(shardId, testNodeToken2, updatedShardInfo);
            var updatedQueryResult = shardSizeService.getCachedShardMetrics(shardId, 2, 2);

            assertThat(queryResult, isPresentWith(new CacheEntry(testNodeToken1, shardInfo)));
            assertThat(updatedQueryResult, isPresentWith(new CacheEntry(testNodeToken2, updatedShardInfo)));
        }
    }
}
