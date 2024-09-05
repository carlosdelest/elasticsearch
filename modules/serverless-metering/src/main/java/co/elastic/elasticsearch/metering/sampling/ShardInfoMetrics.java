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

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ShardInfoMetrics(
    long sizeInBytes,
    long docCount,
    long primaryTerm,
    long generation,
    long storedIngestSizeInBytes,
    long indexCreationDateEpochMilli
) implements Writeable {

    public static final ShardInfoMetrics EMPTY = new ShardInfoMetrics(0, 0, 0, 0, 0, 0);

    public ShardInfoMetrics {
        assert sizeInBytes >= 0 : "size must be non negative";
    }

    public static ShardInfoMetrics from(StreamInput in) throws IOException {
        var sizeInBytes = in.readVLong();
        long docCount = in.readVLong();
        var primaryTerm = in.readVLong();
        var generation = in.readVLong();
        final long storedIngestSizeInBytes;
        long indexCreationDateEpochMilli = 0;
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.SHARD_INFO_INDEX_CREATION_DATE_ADDED)) {
            storedIngestSizeInBytes = in.readVLong();
            indexCreationDateEpochMilli = in.readVLong();
        } else {
            storedIngestSizeInBytes = in.readOptionalVLong();
        }
        return new ShardInfoMetrics(sizeInBytes, docCount, primaryTerm, generation, storedIngestSizeInBytes, indexCreationDateEpochMilli);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeVLong(docCount);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.SHARD_INFO_INDEX_CREATION_DATE_ADDED)) {
            out.writeVLong(storedIngestSizeInBytes);
            out.writeVLong(indexCreationDateEpochMilli);
        } else {
            out.writeOptionalVLong(storedIngestSizeInBytes);
        }
    }

    public boolean isMoreRecentThan(ShardInfoMetrics other) {
        return primaryTerm > other.primaryTerm || (primaryTerm == other.primaryTerm && generation > other.generation);
    }

    public ShardInfoMetrics mostRecent(ShardInfoMetrics other) {
        return isMoreRecentThan(other) ? this : other;
    }
}
