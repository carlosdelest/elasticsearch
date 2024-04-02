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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardReaderTests extends ESTestCase {

    public void testEmptySetWhenNoIndices() throws IOException {

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = mock(LocalNodeMeteringShardInfoCache.class);
        var shardReader = new ShardReader(indicesService);

        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        var shardSizes = shardReader.getMeteringShardInfoMap(shardInfoCache);

        assertThat(shardSizes.keySet(), empty());
    }

    public void testMultipleIndicesReportAllShards() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = mock(LocalNodeMeteringShardInfoCache.class);
        var shardReader = new ShardReader(indicesService);

        var index1 = mock(IndexService.class);
        var index2 = mock(IndexService.class);

        var shard1 = mock(IndexShard.class);
        var shard2 = mock(IndexShard.class);
        var shard3 = mock(IndexShard.class);

        when(indicesService.iterator()).thenReturn(Iterators.concat(Iterators.single(index1), Iterators.single(index2)));
        when(index1.iterator()).thenReturn(Iterators.concat(Iterators.single(shard1), Iterators.single(shard2)));
        when(index2.iterator()).thenReturn(Iterators.single(shard3));

        var engine = mock(Engine.class);
        when(engine.getLastCommittedSegmentInfos()).thenReturn(createMockSegmentInfos(10L));

        when(shard1.getEngineOrNull()).thenReturn(engine);
        when(shard2.getEngineOrNull()).thenReturn(engine);
        when(shard3.getEngineOrNull()).thenReturn(engine);

        when(shard1.shardId()).thenReturn(shardId1);
        when(shard2.shardId()).thenReturn(shardId2);
        when(shard3.shardId()).thenReturn(shardId3);

        var shardInfoMap = shardReader.getMeteringShardInfoMap(shardInfoCache);

        verify(shardInfoCache, times(3)).getCachedShardInfo(any(), anyLong(), anyLong());
        verify(shardInfoCache, times(3)).updateCachedShardInfo(any(), anyLong(), anyLong(), anyLong(), anyLong());

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));
    }

    private static class TestSegmentCommitInfo extends SegmentCommitInfo {

        private final long size;

        TestSegmentCommitInfo(long size) {
            super(
                new SegmentInfo(
                    mock(Directory.class),
                    Version.LATEST,
                    Version.LATEST,
                    "",
                    0,
                    false,
                    false,
                    mock(Codec.class),
                    Map.of(),
                    new byte[16],
                    Map.of(),
                    Sort.INDEXORDER
                ),
                0,
                0,
                0,
                0,
                0,
                null
            );
            this.size = size;
        }

        @Override
        public long sizeInBytes() {
            return size;
        }
    }

    private static SegmentInfos createMockSegmentInfos(long size) {
        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        var segmentInfo = new TestSegmentCommitInfo(size);
        segmentInfos.add(segmentInfo);
        return segmentInfos;
    }
}
