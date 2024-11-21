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

package co.elastic.elasticsearch.stateless.api;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public interface ShardSizeStatsReader {

    Map<ShardId, ShardSize> getAllShardSizes(TimeValue boostWindowInterval);

    @Nullable
    ShardSize getShardSize(IndexShard indexShard, TimeValue boostWindowInterval);

    @Nullable
    ShardSize getShardSize(ShardId shardId, TimeValue boostWindowInterval);

    record ShardSize(long interactiveSizeInBytes, long nonInteractiveSizeInBytes, long primaryTerm, long generation) implements Writeable {

        private static final Comparator<ShardSize> PRIMARY_TERM_AND_GENERATION_COMPARATOR = Comparator.comparing(ShardSize::primaryTerm)
            .thenComparing(ShardSize::generation);

        public static final ShardSize EMPTY = new ShardSize(0, 0, 0, 0);

        public ShardSize {
            assert interactiveSizeInBytes >= 0 : "interactiveSize must be non negative";
            assert nonInteractiveSizeInBytes >= 0 : "nonInteractiveSize must be non negative";
        }

        public static ShardSize from(StreamInput in) throws IOException {
            return new ShardSize(in.readLong(), in.readLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(interactiveSizeInBytes);
            out.writeLong(nonInteractiveSizeInBytes);
            out.writeVLong(primaryTerm);
            out.writeVLong(generation);
        }

        public long totalSizeInBytes() {
            return interactiveSizeInBytes + nonInteractiveSizeInBytes;
        }

        public boolean onOrBefore(ShardSize other) {
            return PRIMARY_TERM_AND_GENERATION_COMPARATOR.compare(this, other) <= 0;
        }

        @Override
        public String toString() {
            return "[interactive_in_bytes="
                + interactiveSizeInBytes
                + ", non-interactive_in_bytes="
                + nonInteractiveSizeInBytes
                + "][term="
                + primaryTerm
                + ", gen="
                + generation
                + ']';
        }
    }
}
