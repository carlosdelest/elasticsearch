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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ShardInfoMetrics(
    long docCount,
    long interactiveSizeInBytes,
    long nonInteractiveSizeInBytes,
    long rawStoredSizeInBytes,
    long primaryTerm,
    long generation,
    long indexCreationDateEpochMilli
) implements Writeable {

    public static final ShardInfoMetrics EMPTY = new ShardInfoMetrics(0, 0, 0L, 0, 0, 0, 0);

    public ShardInfoMetrics {
        assert interactiveSizeInBytes >= 0 : "interactiveSizeInBytes must be non negative";
        assert nonInteractiveSizeInBytes >= 0 : "nonInteractiveSizeInBytes must be non negative";
        assert rawStoredSizeInBytes >= 0 : "rawStoredSizeInBytes must be non negative";
    }

    public long totalSizeInBytes() {
        return interactiveSizeInBytes + nonInteractiveSizeInBytes;
    }

    public static ShardInfoMetrics from(StreamInput in) throws IOException {
        return new ShardInfoMetrics(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(docCount);
        out.writeVLong(interactiveSizeInBytes);
        out.writeVLong(nonInteractiveSizeInBytes);
        out.writeVLong(rawStoredSizeInBytes);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
        out.writeVLong(indexCreationDateEpochMilli);
    }

    public boolean isMoreRecentThan(ShardInfoMetrics other) {
        return primaryTerm > other.primaryTerm || (primaryTerm == other.primaryTerm && generation > other.generation);
    }

    public ShardInfoMetrics mostRecent(ShardInfoMetrics other) {
        return isMoreRecentThan(other) ? this : other;
    }
}
