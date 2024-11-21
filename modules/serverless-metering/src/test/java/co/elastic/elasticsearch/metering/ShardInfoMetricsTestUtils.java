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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.sampling.ShardInfoMetrics;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import static java.time.Instant.EPOCH;

public class ShardInfoMetricsTestUtils {

    public static ShardInfoMetricsBuilder shardInfoMetricsBuilder() {
        return new ShardInfoMetricsBuilder();
    }

    public static class ShardInfoMetricsBuilder {
        long docCount;
        long interactiveSizeInBytes;
        long nonInteractiveSizeInBytes;
        long rawStoredSizeInBytes;
        long primaryTerm;
        long generation;
        long indexCreationDateEpochMilli = EPOCH.toEpochMilli();
        long segmentCount;
        long deletedDocCount;
        ShardInfoMetrics.RawStoredSizeStats raStats = ShardInfoMetrics.RawStoredSizeStats.EMPTY;

        public ShardInfoMetricsBuilder withData(
            long docCount,
            long interactiveSizeInBytes,
            long nonInteractiveSizeInBytes,
            long rawStoredSizeInBytes
        ) {
            this.docCount = docCount;
            this.interactiveSizeInBytes = interactiveSizeInBytes;
            this.nonInteractiveSizeInBytes = nonInteractiveSizeInBytes;
            this.rawStoredSizeInBytes = rawStoredSizeInBytes;
            return this;
        }

        public ShardInfoMetricsBuilder withData(ShardInfoMetrics shardInfoMetrics) {
            this.docCount = shardInfoMetrics.docCount();
            this.interactiveSizeInBytes = shardInfoMetrics.interactiveSizeInBytes();
            this.nonInteractiveSizeInBytes = shardInfoMetrics.nonInteractiveSizeInBytes();
            this.rawStoredSizeInBytes = shardInfoMetrics.rawStoredSizeInBytes();
            return this;
        }

        public ShardInfoMetricsBuilder withGeneration(long primaryTerm, long generation, long indexCreationDateEpochMilli) {
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.indexCreationDateEpochMilli = indexCreationDateEpochMilli;
            return this;
        }

        public ShardInfoMetricsBuilder withGeneration(ShardInfoMetrics shardInfoMetrics) {
            this.primaryTerm = shardInfoMetrics.primaryTerm();
            this.generation = shardInfoMetrics.generation();
            this.indexCreationDateEpochMilli = shardInfoMetrics.indexCreationDateEpochMilli();
            return this;
        }

        public ShardInfoMetricsBuilder withRAStats(
            long segmentCount,
            long deletedDocCount,
            long raSegmentCount,
            long raLiveDocCount,
            long raDeletedDocCount,
            long raApproximatedDocCount,
            long rawStoredSizeAvgMin,
            long rawStoredSizeAvgMax,
            double rawStoredSizeAvgTotal,
            double rawStoredSizeAvgTotal2
        ) {
            this.segmentCount = segmentCount;
            this.deletedDocCount = deletedDocCount;
            this.raStats = new ShardInfoMetrics.RawStoredSizeStats(
                raSegmentCount,
                raLiveDocCount,
                raDeletedDocCount,
                raApproximatedDocCount,
                rawStoredSizeAvgMin,
                rawStoredSizeAvgMax,
                rawStoredSizeAvgTotal,
                rawStoredSizeAvgTotal2
            );
            return this;
        }

        public ShardInfoMetrics build() {
            return new ShardInfoMetrics(
                docCount,
                interactiveSizeInBytes,
                nonInteractiveSizeInBytes,
                rawStoredSizeInBytes,
                primaryTerm,
                generation,
                indexCreationDateEpochMilli,
                segmentCount,
                deletedDocCount,
                raStats
            );
        }

    }

    public static Matcher<ShardInfoMetrics> matchesDataAndGeneration(ShardInfoMetrics item) {
        return new FeatureMatcher<>(Matchers.equalTo(item), "has metrics data", "shardInfoMetricsData") {
            @Override
            protected ShardInfoMetrics featureValueOf(ShardInfoMetrics shardInfoMetrics) {
                return shardInfoMetricsBuilder().withData(shardInfoMetrics).withGeneration(shardInfoMetrics).build();
            }
        };
    }
}
