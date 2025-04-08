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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.ShardKey;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.ShardSample;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

final class IndexInfoMetrics {
    private static final Collector<Map.Entry<ShardKey, ShardSample>, ?, Map<String, IndexInfoMetrics>> INDEX_SAMPLES_COLLECTOR = groupingBy(
        e -> e.getKey().indexName(),
        Collector.of(IndexInfoMetrics::new, IndexInfoMetrics::accumulate, IndexInfoMetrics::combine)
    );

    private Instant indexCreationDate;
    private long totalSize;
    private long interactiveSize;

    private long rawStorageSize;

    private long segmentCount;
    private long liveDocCount;
    private long deletedDocCount;

    private long rawSegmentCount;
    private long rawLiveDocCount;
    private long rawDeletedDocCount;
    private long rawApproximatedDocCount;
    private long rawAvgMin = Long.MAX_VALUE;
    private long rawAvgMax = 0;
    private double rawAvgTotal;
    private double rawAvgSquaredTotal;

    private boolean hasRawStats = false;

    public static Map<String, IndexInfoMetrics> calculateIndexSamples(Map<ShardKey, ShardSample> shardSamples) {
        return shardSamples.entrySet().stream().collect(INDEX_SAMPLES_COLLECTOR);
    }

    private void accumulate(Map.Entry<ShardKey, ShardSample> t) {
        var shardInfo = t.getValue().shardInfo();

        totalSize += shardInfo.totalSizeInBytes();
        interactiveSize += shardInfo.interactiveSizeInBytes();
        rawStorageSize += shardInfo.rawStoredSizeInBytes();

        segmentCount += shardInfo.segmentCount();
        liveDocCount += shardInfo.docCount();
        deletedDocCount += shardInfo.deletedDocCount();

        if (shardInfo.rawStoredSizeStats().isEmpty() == false) {
            hasRawStats = true;
            rawSegmentCount += shardInfo.rawStoredSizeStats().segmentCount();
            rawLiveDocCount += shardInfo.rawStoredSizeStats().liveDocCount();
            rawDeletedDocCount += shardInfo.rawStoredSizeStats().deletedDocCount();
            rawApproximatedDocCount += shardInfo.rawStoredSizeStats().approximatedDocCount();
            rawAvgMin = Math.min(rawAvgMin, shardInfo.rawStoredSizeStats().avgMin());
            rawAvgMax = Math.max(rawAvgMax, shardInfo.rawStoredSizeStats().avgMax());
            rawAvgTotal += shardInfo.rawStoredSizeStats().avgTotal();
            rawAvgSquaredTotal += shardInfo.rawStoredSizeStats().avgSquaredTotal();
        }

        indexCreationDate = getEarlierValidCreationDate(indexCreationDate, Instant.ofEpochMilli(shardInfo.indexCreationDateEpochMilli()));
    }

    private IndexInfoMetrics combine(IndexInfoMetrics b) {
        totalSize += b.totalSize;
        interactiveSize += b.interactiveSize;
        rawStorageSize += b.rawStorageSize;

        segmentCount += b.segmentCount;
        liveDocCount += b.liveDocCount;
        deletedDocCount += b.deletedDocCount;

        rawSegmentCount += b.rawSegmentCount;
        rawLiveDocCount += b.rawLiveDocCount;
        rawDeletedDocCount += b.rawDeletedDocCount;
        rawApproximatedDocCount += b.rawApproximatedDocCount;
        rawAvgMin = Math.min(rawAvgMin, b.rawAvgMin);
        rawAvgMax = Math.max(rawAvgMax, b.rawAvgMax);
        rawAvgTotal += b.rawAvgTotal;
        rawAvgSquaredTotal += b.rawAvgSquaredTotal;

        indexCreationDate = getEarlierValidCreationDate(indexCreationDate, b.indexCreationDate);
        return this;
    }

    private static Instant getEarlierValidCreationDate(Instant a, Instant b) {
        if (a == null || (b != null && b.isBefore(a))) {
            return b;
        }
        return a;
    }

    public long getDeletedDocCount() {
        return deletedDocCount;
    }

    public boolean hasRawStats() {
        return hasRawStats;
    }

    public Instant getIndexCreationDate() {
        return indexCreationDate;
    }

    public long getInteractiveSize() {
        return interactiveSize;
    }

    public long getLiveDocCount() {
        return liveDocCount;
    }

    public long getRawApproximatedDocCount() {
        return rawApproximatedDocCount;
    }

    public long getRawAvgMax() {
        return rawAvgMax;
    }

    public long getRawAvgMin() {
        return rawAvgMin;
    }

    public long getRawAvgStddev() {
        return (long) Math.sqrt(rawAvgSquaredTotal / rawSegmentCount - Math.pow(getRawAvgAvg(), 2));
    }

    public long getRawAvgAvg() {
        return (long) rawAvgTotal / rawSegmentCount;
    }

    public long getRawDeletedDocCount() {
        return rawDeletedDocCount;
    }

    public long getRawLiveDocCount() {
        return rawLiveDocCount;
    }

    public long getRawSegmentCount() {
        return rawSegmentCount;
    }

    public long getRawStorageSize() {
        return rawStorageSize;
    }

    public long getSegmentCount() {
        return segmentCount;
    }

    public long getTotalSize() {
        return totalSize;
    }
}
