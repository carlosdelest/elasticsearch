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

import co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils;
import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;
import co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsProvider;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils.matchesDataAndGeneration;
import static co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator.RA_STORAGE_AVG_KEY;
import static co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator.RA_STORAGE_KEY;
import static co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader.SHARD_INFO_CACHED_TOTAL_METRIC;
import static co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader.SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC;
import static co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader.SHARD_INFO_RA_STORAGE_NEWER_GEN_TOTAL_METRIC;
import static co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader.SHARD_INFO_SHARDS_TOTAL_METRIC;
import static co.elastic.elasticsearch.metering.sampling.action.ShardInfoMetricsReader.DefaultShardInfoMetricsReader.SHARD_INFO_UNAVAILABLE_TOTAL_METRIC;
import static java.util.Collections.emptyIterator;
import static org.elasticsearch.telemetry.InstrumentType.DOUBLE_HISTOGRAM;
import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class ShardInfoMetricsReaderTests extends ESTestCase {

    private static final int GEN_1 = 1;
    private static final int GEN_2 = 2;

    private static final ShardSize EMPTY_SHARD_SIZE_OLD_PRIMARY = new ShardSize(0, 0, 0, 0);
    private static final ShardSize EMPTY_SHARD_SIZE_GEN_1 = new ShardSize(0, 0, 1, GEN_1);
    private static final ShardInfoMetrics EMPTY_SHARD_INFO_GEN_1 = ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
        .withGeneration(1, GEN_1, 0)
        .build();

    private final IndicesService indicesService = mock();
    private final ShardSizeStatsProvider shardSizeStatsProvider = mock();
    private final InMemoryShardInfoMetricsCache shardInfoCache = spy(new InMemoryShardInfoMetricsCache());
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final DefaultShardInfoMetricsReader shardReader = new DefaultShardInfoMetricsReader(
        indicesService,
        shardSizeStatsProvider,
        shardInfoCache,
        meterRegistry
    );

    public void testEmptySetWhenNoIndices() {
        when(indicesService.iterator()).thenReturn(emptyIterator());

        var shardSizes = shardReader.getUpdatedShardInfos("TEST-NODE");
        assertThat(shardSizes.keySet(), empty());

        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_SHARDS_TOTAL_METRIC), empty());
    }

    public void testMultipleIndicesReportAllShards() {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        when(shardSizeStatsProvider.getShardSize(any())).thenReturn(EMPTY_SHARD_SIZE_GEN_1);

        var index1 = createMockIndexService(Map.of(shardId1, segmentInfos(GEN_1), shardId2, segmentInfos(GEN_1)));
        var index2 = createMockIndexService(Map.of(shardId3, segmentInfos(GEN_2)));

        when(indicesService.iterator()).thenReturn(List.of(index1, index2).iterator());

        var shardInfoMap = shardReader.getUpdatedShardInfos("TEST-NODE");

        verify(shardInfoCache, times(3)).getCachedShardMetrics(any(), anyLong(), anyLong());
        verify(shardInfoCache, times(3)).updateCachedShardMetrics(any(), eq("TEST-NODE"), any());

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));

        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_SHARDS_TOTAL_METRIC), contains(measurement(is(3L))));
        // shard3 has progressed to generation 2
        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_RA_STORAGE_NEWER_GEN_TOTAL_METRIC), contains(measurement(is(1L))));
    }

    public void testSkipUnavailableShards() {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);
        ShardId shardId4 = new ShardId("index2", "index2UUID", 2);

        when(shardSizeStatsProvider.getShardSize(argThat(oneOf(shardId1, shardId2)))).thenReturn(EMPTY_SHARD_SIZE_GEN_1);
        // no shard size available for shardId3, shardId4 skipped due to old primary
        when(shardSizeStatsProvider.getShardSize(shardId4)).thenReturn(EMPTY_SHARD_SIZE_OLD_PRIMARY);

        var index1 = createMockIndexService(Map.of(shardId1, segmentInfos(GEN_1), shardId2, segmentInfos(GEN_1)));
        var index2 = createMockIndexService(Map.of(shardId3, segmentInfos(GEN_1), shardId4, segmentInfos(GEN_1)));

        // shardId2 turned unavailable
        when(index1.getShard(shardId2.id()).getEngineOrNull()).thenReturn(null);

        when(indicesService.iterator()).thenReturn(List.of(index1, index2).iterator());

        var shardInfoMap = shardReader.getUpdatedShardInfos("TEST-NODE");

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1)); // other shards are unavailable

        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_SHARDS_TOTAL_METRIC), contains(measurement(is(4L))));
        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_UNAVAILABLE_TOTAL_METRIC), contains(measurement(is(3L))));
    }

    public void testCacheUpdatedWhenIndexDeleted() {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        // irrelevant for this test
        when(shardSizeStatsProvider.getShardSize(any(ShardId.class))).thenReturn(EMPTY_SHARD_SIZE_GEN_1);

        var index1 = createMockIndexService(Map.of(shardId1, segmentInfos(GEN_1), shardId2, segmentInfos(GEN_1)));
        var index2 = createMockIndexService(Map.of(shardId3, segmentInfos(GEN_1)));

        when(indicesService.iterator()).thenReturn(List.of(index1, index2).iterator());

        var shardInfoMap = shardReader.getUpdatedShardInfos("TEST-NODE");
        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));
        assertThat(shardInfoCache.shardMetricsCache.keySet(), containsInAnyOrder(shardId1, shardId2, shardId3));

        // pretend index1 was deleted (not included in iterator anymore)
        when(indicesService.iterator()).thenReturn(Iterators.single(index2));

        shardInfoMap = shardReader.getUpdatedShardInfos("TEST-NODE");
        assertThat(shardInfoMap.keySet(), empty());
        // shards of index1 got dropped from the cache
        assertThat(shardInfoCache.shardMetricsCache.keySet(), contains(shardId3));

        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_SHARDS_TOTAL_METRIC), contains(measurement(is(4L))));
        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_CACHED_TOTAL_METRIC), contains(measurement(is(1L))));
    }

    public void testNotReturningUnchangedDataInDiff() {
        ShardId shardId1 = new ShardId("index1", "index1UUID", 1);
        ShardId shardId2 = new ShardId("index1", "index1UUID", 2);
        ShardId shardId3 = new ShardId("index2", "index2UUID", 1);

        when(shardSizeStatsProvider.getShardSize(any(ShardId.class))).thenReturn(EMPTY_SHARD_SIZE_GEN_1);
        shardInfoCache.updateCachedShardMetrics(shardId3, "TEST-NODE", EMPTY_SHARD_INFO_GEN_1);

        var index1 = createMockIndexService(Map.of(shardId1, segmentInfos(GEN_1), shardId2, segmentInfos(GEN_1)));
        var index2 = createMockIndexService(Map.of(shardId3, segmentInfos(GEN_1)));

        when(indicesService.iterator()).thenReturn(List.of(index1, index2).iterator());

        var shardInfoMap = shardReader.getUpdatedShardInfos("TEST-NODE");

        verify(shardInfoCache, times(3)).getCachedShardMetrics(any(), anyLong(), anyLong());
        // updateCachedShardInfo is not invoked when both generation and requestToken are up-to-date
        // we get 2 new updates in addition to the one when setting up the test
        verify(shardInfoCache, times(3)).updateCachedShardMetrics(any(), eq("TEST-NODE"), any());

        assertThat(shardInfoMap.keySet(), containsInAnyOrder(shardId1, shardId2));

        assertThat(getMeasurements(LONG_COUNTER, SHARD_INFO_CACHED_TOTAL_METRIC), contains(measurement(is(1L))));
    }

    public void testComputeShardInfoWithoutRAS() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(GEN_1, segmentCommitInfo(100, 0, 0, null), segmentCommitInfo(50, 10, 20, null));

        ShardSize shardSize = new ShardSize(10L, 20L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        assertThat(
            shardInfo,
            matchesDataAndGeneration(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(120L, 10L, 20L, 0)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .build()
            )
        );
        assertTrue(shardInfo.rawStoredSizeStats().isEmpty());
        assertThat(getMeasurements(DOUBLE_HISTOGRAM, SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC), empty());
    }

    public void testComputeShardInfoWithFullRASMultipleSegments() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(GEN_1, segmentCommitInfo(10, 0, 0, 8L), segmentCommitInfo(50, 10, 20, 6L));

        ShardSize shardSize = new ShardSize(180L, 120L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        assertThat(
            shardInfo,
            equalTo(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(30L, 180L, 120L, 8 * 10L + 20 * 6L)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .withRAStats(2, 30, 2, 30, 30, 20, 6, 8, 14, 100)
                    .build()
            )
        );

        assertThat(getMeasurements(DOUBLE_HISTOGRAM, SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC), contains(measurement(is((0.5)))));
    }

    public void testComputeShardInfoWithInvalidRASSegment() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(
            GEN_1,
            segmentCommitInfo(10, 0, 0, 8L),
            // This segment has an invalid RA-S avg value (due to ES-9361)
            segmentCommitInfo(50, 10, 20, -1L)
        );

        ShardSize shardSize = new ShardSize(180L, 120L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        // invalid segment is not included in the RA-S calculation
        assertThat(
            shardInfo,
            matchesDataAndGeneration(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(30L, 180L, 120L, 8 * 10L)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .build()
            )
        );

        assertThat(shardInfo.segmentCount(), is(2L));
        assertThat(shardInfo.rawStoredSizeStats().segmentCount(), is(1L));
        assertThat(shardInfo.rawStoredSizeStats().avgMax(), is(8L));
        assertThat(shardInfo.rawStoredSizeStats().avgMin(), is(8L));
        assertThat(shardInfo.rawStoredSizeStats().avgTotal(), is(8.0));
    }

    public void testComputeShardInfoWithPartialRASMultipleSegments() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(
            GEN_1,
            segmentCommitInfo(10, 0, 0, 8L),
            segmentCommitInfo(10, 0, 0, null),
            segmentCommitInfo(50, 0, 0, 11L),
            segmentCommitInfo(50, 10, 20, 6L)
        );

        ShardSize shardSize = new ShardSize(400L, 200L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        assertThat(
            shardInfo,
            equalTo(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(90L, 400L, 200L, 80L + 550L + 120L)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .withRAStats(4, 30, 3, 80, 30, 20, 6, 11, 25, 221)
                    .build()
            )
        );

        assertThat(getMeasurements(DOUBLE_HISTOGRAM, SHARD_INFO_RA_STORAGE_APPROXIMATED_METRIC), contains(measurement(is(1.0 / 3.0))));
    }

    public void testComputeShardInfoWithTimeseriesRASMultipleSegments() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(
            GEN_1,
            Map.of(RA_STORAGE_KEY, "234"),
            segmentCommitInfo(100, 0, 0, null),
            segmentCommitInfo(50, 10, 20, null)
        );

        ShardSize shardSize = new ShardSize(15L, 15L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        assertThat(
            shardInfo,
            matchesDataAndGeneration(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(120L, 15L, 15L, 234L)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .build()
            )
        );

        assertThat(shardInfo.segmentCount(), is(2L));
        assertThat(shardInfo.rawStoredSizeStats().segmentCount(), is(0L));
    }

    public void testComputeShardInfoWithInvalidTimeseriesRAS() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        // The shard has an invalid RA-S value (due to ES-9361)
        var segmentInfos = segmentInfos(GEN_1, Map.of(RA_STORAGE_KEY, "-100"));

        // shard size is irrelevant for this test
        var shardInfo = shardReader.computeShardInfo(shardId, EMPTY_SHARD_SIZE_GEN_1, 0, segmentInfos);

        assertThat(shardInfo.rawStoredSizeInBytes(), equalTo(0L));
        assertTrue(shardInfo.rawStoredSizeStats().isEmpty());
    }

    public void testComputeShardStatsPerSegmentRASHasPrecedenceOverPerShardRAS() {
        ShardId shardId = new ShardId("index1", "index1UUID", 1);

        var segmentInfos = segmentInfos(
            GEN_1,
            Map.of(RA_STORAGE_KEY, "234"),
            segmentCommitInfo(10, 0, 0, null),
            segmentCommitInfo(50, 10, 20, 6L)
        );

        ShardSize shardSize = new ShardSize(180L, 120L, 1, GEN_1);

        var timestampMillis = randomNonNegativeLong();
        var shardInfo = shardReader.computeShardInfo(shardId, shardSize, timestampMillis, segmentInfos);
        assertThat(
            shardInfo,
            matchesDataAndGeneration(
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder()
                    .withData(30L, 180L, 120L, 120L)
                    .withGeneration(1L, GEN_1, timestampMillis)
                    .build()
            )
        );
    }

    private static SegmentCommitInfo segmentCommitInfo(int maxDoc, int delCount, int softDelCount, Long segmentRASize) {
        var segmentInfo = new SegmentInfo(
            mock(Directory.class),
            Version.LATEST,
            Version.LATEST,
            "name",
            maxDoc,
            false,
            false,
            mock(Codec.class),
            Map.of(),
            new byte[16],
            segmentRASize != null ? Map.of(RA_STORAGE_AVG_KEY, Long.toString(segmentRASize)) : Map.of(),
            Sort.INDEXORDER
        );
        return new SegmentCommitInfo(segmentInfo, delCount, softDelCount, 0, 0, 0, null);
    }

    private static SegmentInfos segmentInfos(long generation, SegmentCommitInfo... commitInfos) {
        return segmentInfos(generation, null, commitInfos);
    }

    private static SegmentInfos segmentInfos(long generation, Map<String, String> userData, SegmentCommitInfo... commitInfos) {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.setNextWriteGeneration(generation);
        segmentInfos.setUserData(userData, false);
        segmentInfos.addAll(Arrays.asList(commitInfos));
        return segmentInfos;
    }

    private static IndexService createMockIndexService(Map<ShardId, SegmentInfos> shards) {
        var index = mock(IndexService.class);
        List<IndexShard> indexShards = shards.entrySet().stream().map(entries -> {
            var shard = mock(IndexShard.class);
            var engine = mock(Engine.class);
            when(index.getShard(entries.getKey().id())).thenReturn(shard);
            when(shard.getOperationPrimaryTerm()).thenReturn(1L);
            when(shard.getEngineOrNull()).thenReturn(engine);
            when(shard.shardId()).thenReturn(entries.getKey());
            when(engine.getLastCommittedSegmentInfos()).thenReturn(entries.getValue());
            return shard;
        }).toList();
        when(index.iterator()).thenAnswer(inv -> indexShards.iterator());
        var metadata = mock(IndexMetadata.class);
        when(metadata.getCreationDate()).thenReturn(0L);
        when(index.getMetadata()).thenReturn(metadata);
        return index;
    }

    private List<Measurement> getMeasurements(InstrumentType type, String name) {
        return Measurement.combine(meterRegistry.getRecorder().getMeasurements(type, name));
    }

    private static Matcher<Measurement> measurement(Matcher<Number> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "a measurement of", "value") {
            @Override
            protected Number featureValueOf(Measurement measurement) {
                if (measurement.isDouble()) {
                    return measurement.getDouble();
                } else {
                    return measurement.getLong();
                }
            }
        };
    }
}
