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
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageAccumulator.RA_STORAGE_AVG_KEY;
import static co.elastic.elasticsearch.metering.ingested_size.reporter.RAStorageAccumulator.RA_STORAGE_KEY;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardReaderTests extends ESTestCase {

    public void testEmptySetWhenNoIndices() throws IOException {

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = mock(LocalNodeMeteringShardInfoCache.class);
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);

        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        var shardSizes = shardReader.getMeteringShardInfoMap(shardInfoCache, "TEST-NODE");

        assertThat(shardSizes.keySet(), empty());
        final List<Measurement> measurements = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ShardReader.SHARD_INFO_REQUESTS_TOTAL_METRIC)
        );
        assertThat(measurements, empty());
    }

    public void testMultipleIndicesReportAllShards() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = mock(LocalNodeMeteringShardInfoCache.class);
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);

        var index1 = mock(IndexService.class);
        var index2 = mock(IndexService.class);

        var shard1 = mock(IndexShard.class);
        var shard2 = mock(IndexShard.class);
        var shard3 = mock(IndexShard.class);

        when(indicesService.iterator()).thenReturn(Iterators.concat(Iterators.single(index1), Iterators.single(index2)));
        when(index1.iterator()).thenReturn(Iterators.concat(Iterators.single(shard1), Iterators.single(shard2)));
        when(index2.iterator()).thenReturn(Iterators.single(shard3));

        var engine = mock(Engine.class);
        when(engine.getLastCommittedSegmentInfos()).thenReturn(createMockSegmentInfos(11L));

        when(shard1.getEngineOrNull()).thenReturn(engine);
        when(shard2.getEngineOrNull()).thenReturn(engine);
        when(shard3.getEngineOrNull()).thenReturn(engine);

        when(shard1.shardId()).thenReturn(shardId1);
        when(shard2.shardId()).thenReturn(shardId2);
        when(shard3.shardId()).thenReturn(shardId3);

        var shardInfoMap = shardReader.getMeteringShardInfoMap(shardInfoCache, "TEST-NODE");

        verify(shardInfoCache, times(3)).getCachedShardInfo(any(), anyLong(), anyLong());
        verify(shardInfoCache, times(3)).updateCachedShardInfo(
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            eq("TEST-NODE"),
            anyLong()
        );

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));
        final List<Measurement> measurements = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ShardReader.SHARD_INFO_REQUESTS_TOTAL_METRIC)
        );
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(3L));
    }

    public void testCacheUpdatedWhenIndexDeleted() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = new LocalNodeMeteringShardInfoCache();
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);

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

        var shardInfoMap = shardReader.getMeteringShardInfoMap(shardInfoCache, "TEST-NODE");
        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));
        assertThat(shardInfoCache.shardSizeCache.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));

        when(indicesService.iterator()).thenReturn(Iterators.single(index2));
        when(index2.iterator()).thenReturn(Iterators.single(shard3));

        shardInfoMap = shardReader.getMeteringShardInfoMap(shardInfoCache, "TEST-NODE");
        assertThat(shardInfoMap.keySet(), empty());
        assertThat(shardInfoCache.shardSizeCache.keySet(), contains(shardId3));

        final List<Measurement> requests = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ShardReader.SHARD_INFO_REQUESTS_TOTAL_METRIC)
        );
        assertThat(requests, hasSize(1));
        assertThat(requests.get(0).getLong(), equalTo(4L));

        final List<Measurement> cached = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ShardReader.SHARD_INFO_CACHED_TOTAL_METRIC)
        );
        assertThat(cached, hasSize(1));
        assertThat(cached.get(0).getLong(), equalTo(1L));
    }

    public void testNotReturningUnchangedDataInDiff() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        var indicesService = mock(IndicesService.class);
        var shardInfoCache = mock(LocalNodeMeteringShardInfoCache.class);
        when(shardInfoCache.getCachedShardInfo(eq(shardId3), anyLong(), anyLong())).thenReturn(
            Optional.of(new LocalNodeMeteringShardInfoCache.CacheEntry(1L, 1L, 10L, 100L, "TEST-NODE", 0))
        );

        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);

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

        var shardInfoMap = shardReader.getMeteringShardInfoMap(shardInfoCache, "TEST-NODE");

        verify(shardInfoCache, times(3)).getCachedShardInfo(any(), anyLong(), anyLong());
        // updateCachedShardInfo is not invoked when both generation and requestToken are up-to-date
        verify(shardInfoCache, times(2)).updateCachedShardInfo(
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            eq("TEST-NODE"),
            anyLong()
        );

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2));

        final List<Measurement> cached = Measurement.combine(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ShardReader.SHARD_INFO_CACHED_TOTAL_METRIC)
        );
        assertThat(cached, hasSize(1));
        assertThat(cached.get(0).getLong(), equalTo(1L));
    }

    public void testComputeShardStatsWithoutRA() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(new TestSegmentCommitInfo(10L, 100, 0, 0, null));
        segmentInfos.add(new TestSegmentCommitInfo(20L, 50, 10, 20, null));

        var indicesService = mock(IndicesService.class);
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);
        var shardStats = shardReader.computeShardStats(shardId1, segmentInfos);

        assertThat(shardStats.liveDocCount(), equalTo(120L));
        assertThat(shardStats.sizeInBytes(), equalTo(30L));
        assertThat(shardStats.raSizeInBytes(), equalTo(0L));

        final List<Measurement> approximated = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, ShardReader.SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC);
        assertThat(approximated, empty());
    }

    public void testComputeShardStatsWithFullRAMultipleSegments() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(new TestSegmentCommitInfo(100L, 10, 0, 0, 8L));
        segmentInfos.add(new TestSegmentCommitInfo(200L, 50, 10, 20, 6L));

        var indicesService = mock(IndicesService.class);
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);
        var shardStats = shardReader.computeShardStats(shardId1, segmentInfos);

        assertThat(shardStats.liveDocCount(), equalTo(30L));
        assertThat(shardStats.sizeInBytes(), equalTo(300L));
        assertThat(shardStats.raSizeInBytes(), equalTo(80L + 120L));

        final List<Measurement> approximated = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, ShardReader.SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC);
        assertThat(approximated, contains(transformedMatch(Measurement::getDouble, equalTo(0.5))));
    }

    public void testComputeShardStatsWithPartialRAMultipleSegments() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(new TestSegmentCommitInfo(100L, 10, 0, 0, 8L));
        segmentInfos.add(new TestSegmentCommitInfo(100L, 10, 0, 0, null));
        segmentInfos.add(new TestSegmentCommitInfo(200L, 50, 0, 0, 11L));
        segmentInfos.add(new TestSegmentCommitInfo(200L, 50, 10, 20, 6L));

        var indicesService = mock(IndicesService.class);
        var meterRegistry = new RecordingMeterRegistry();
        var shardReader = new ShardReader(indicesService, meterRegistry);
        var shardStats = shardReader.computeShardStats(shardId1, segmentInfos);

        assertThat(shardStats.liveDocCount(), equalTo(90L));
        assertThat(shardStats.sizeInBytes(), equalTo(600L));
        assertThat(shardStats.raSizeInBytes(), equalTo(80L + 550L + 120L));

        final List<Measurement> approximated = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, ShardReader.SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC);
        assertThat(approximated, contains(transformedMatch(Measurement::getDouble, equalTo(0.25))));
    }

    public void testComputeShardStatsWithTimeseriesRAMultipleSegments() throws IOException {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(new TestSegmentCommitInfo(10L, 100, 0, 0, null));
        segmentInfos.add(new TestSegmentCommitInfo(20L, 50, 10, 20, null));
        segmentInfos.setUserData(Map.of(RA_STORAGE_KEY, "234"), false);

        var indicesService = mock(IndicesService.class);
        var shardReader = new ShardReader(indicesService, MeterRegistry.NOOP);
        var shardStats = shardReader.computeShardStats(shardId1, segmentInfos);

        assertThat(shardStats.liveDocCount(), equalTo(120L));
        assertThat(shardStats.sizeInBytes(), equalTo(30L));
        assertThat(shardStats.raSizeInBytes(), equalTo(234L));
    }

    public void testComputeShardStatsPerSegmentRAHasPrecedenceOverPerShardRA() throws IOException {

        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(new TestSegmentCommitInfo(100L, 10, 0, 0, null));
        segmentInfos.add(new TestSegmentCommitInfo(200L, 50, 10, 20, 6L));
        segmentInfos.setUserData(Map.of(RA_STORAGE_KEY, "234"), false);

        var indicesService = mock(IndicesService.class);
        var shardReader = new ShardReader(indicesService, MeterRegistry.NOOP);
        var shardStats = shardReader.computeShardStats(shardId1, segmentInfos);

        assertThat(shardStats.liveDocCount(), equalTo(30L));
        assertThat(shardStats.sizeInBytes(), equalTo(300L));
        assertThat(shardStats.raSizeInBytes(), equalTo(120L));
    }

    private static class TestSegmentCommitInfo extends SegmentCommitInfo {

        private final long size;

        TestSegmentCommitInfo(long size, int maxDoc, int delCount, int softDelCount, Long segmentRASize) {
            super(
                new SegmentInfo(
                    mock(Directory.class),
                    Version.LATEST,
                    Version.LATEST,
                    "",
                    maxDoc,
                    false,
                    false,
                    mock(Codec.class),
                    Map.of(),
                    new byte[16],
                    segmentRASize != null ? Map.of(RA_STORAGE_AVG_KEY, Long.toString(segmentRASize)) : Map.of(),
                    Sort.INDEXORDER
                ),
                delCount,
                softDelCount,
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
        var segmentInfo = new TestSegmentCommitInfo(size, 100, 0, 0, null);
        segmentInfos.add(segmentInfo);
        return segmentInfos;
    }
}
