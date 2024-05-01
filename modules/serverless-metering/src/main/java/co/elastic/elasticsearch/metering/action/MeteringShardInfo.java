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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record MeteringShardInfo(long sizeInBytes, long docCount, long primaryTerm, long generation) implements Writeable {

    public static final MeteringShardInfo EMPTY = new MeteringShardInfo(0, 0, 0, 0);

    public MeteringShardInfo {
        assert sizeInBytes >= 0 : "size must be non negative";
    }

    public static MeteringShardInfo from(StreamInput in) throws IOException {
        var sizeInBytes = in.readVLong();

        long docCount = in.readVLong();
        var primaryTerm = in.readVLong();
        var generation = in.readVLong();

        return new MeteringShardInfo(sizeInBytes, docCount, primaryTerm, generation);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeVLong(docCount);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
    }
}
