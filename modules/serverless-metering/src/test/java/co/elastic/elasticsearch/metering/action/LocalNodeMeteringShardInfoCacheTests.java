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

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.is;

public class LocalNodeMeteringShardInfoCacheTests extends ESTestCase {

    public void testSizeIsCached() {
        var shardId1 = new ShardId("index1", "index1UUID", 1);
        var shardId2 = new ShardId("index1", "index1UUID", 2);

        final String testNodeToken = "TEST-NODE";

        try (var shardSizeService = new LocalNodeMeteringShardInfoCache()) {
            var shard1QueryResult = shardSizeService.getCachedShardInfo(shardId1, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId1, 1, 1, 10L, 100L, testNodeToken);
            var secondShard1QueryResult = shardSizeService.getCachedShardInfo(shardId1, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId1, 1, 1, 11L, 110L, testNodeToken);
            var thirdShard1QueryResult = shardSizeService.getCachedShardInfo(shardId1, 1, 1);

            var shard2QueryResult = shardSizeService.getCachedShardInfo(shardId2, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId2, 1, 1, 20L, 200L, testNodeToken);
            var secondShard2QueryResult = shardSizeService.getCachedShardInfo(shardId2, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId2, 1, 1, 21L, 210L, testNodeToken);
            var thirdShard2QueryResult = shardSizeService.getCachedShardInfo(shardId2, 1, 1);

            assertThat(shard1QueryResult, isEmpty());
            assertThat(secondShard1QueryResult, isPresent());
            assertThat(secondShard1QueryResult.get().sizeInBytes(), is(10L));
            assertThat(secondShard1QueryResult.get().docCount(), is(100L));
            assertThat(thirdShard1QueryResult, isPresent());
            assertThat(thirdShard1QueryResult.get().sizeInBytes(), is(11L));
            assertThat(thirdShard1QueryResult.get().docCount(), is(110L));

            assertThat(shard2QueryResult, isEmpty());
            assertThat(secondShard2QueryResult, isPresent());
            assertThat(secondShard2QueryResult.get().sizeInBytes(), is(20L));
            assertThat(secondShard2QueryResult.get().docCount(), is(200L));
            assertThat(thirdShard2QueryResult, isPresent());
            assertThat(thirdShard2QueryResult.get().sizeInBytes(), is(21L));
            assertThat(thirdShard2QueryResult.get().docCount(), is(210L));
        }
    }

    public void testCachedSizeNonMatchingWithDifferentPrimaryTermOrGeneration() {
        var shardId = new ShardId("index1", "index1UUID", 1);

        final String testNodeToken = "TEST-NODE";

        try (var shardSizeService = new LocalNodeMeteringShardInfoCache()) {
            var firstQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId, 1, 1, 10L, 100L, testNodeToken);
            var secondQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 1);
            var differentPrimaryQueryResult = shardSizeService.getCachedShardInfo(shardId, 2, 1);
            var differentGenerationQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 2);
            shardSizeService.updateCachedShardInfo(shardId, 2, 2, 11L, 110L, testNodeToken);
            var updatedQueryResult = shardSizeService.getCachedShardInfo(shardId, 2, 2);

            assertThat(firstQueryResult, isEmpty());
            assertThat(secondQueryResult, isPresent());
            assertThat(differentPrimaryQueryResult, isEmpty());
            assertThat(differentGenerationQueryResult, isEmpty());
            assertThat(updatedQueryResult, isPresent());
            assertThat(updatedQueryResult.get().sizeInBytes(), is(11L));
            assertThat(updatedQueryResult.get().docCount(), is(110L));
        }
    }

    public void testCachedSizeMatchingWithDifferentToken() {
        var shardId = new ShardId("index1", "index1UUID", 1);

        final String testNodeToken1 = "TEST-NODE1";
        final String testNodeToken2 = "TEST-NODE2";

        try (var shardSizeService = new LocalNodeMeteringShardInfoCache()) {
            var firstQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId, 1, 1, 10L, 100L, testNodeToken1);
            var secondQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 1);
            var differentTokenQueryResult = shardSizeService.getCachedShardInfo(shardId, 1, 1);
            shardSizeService.updateCachedShardInfo(shardId, 2, 2, 11L, 110L, testNodeToken2);
            var updatedQueryResult = shardSizeService.getCachedShardInfo(shardId, 2, 2);

            assertThat(firstQueryResult, isEmpty());
            assertThat(secondQueryResult, isPresent());
            assertThat(differentTokenQueryResult, isPresent());
            assertThat(updatedQueryResult, isPresent());
            assertThat(updatedQueryResult.get().sizeInBytes(), is(11L));
            assertThat(updatedQueryResult.get().docCount(), is(110L));
        }
    }
}
