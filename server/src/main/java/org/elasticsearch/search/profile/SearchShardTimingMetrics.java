/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-shard wall-clock timing for each search phase. Each phase is its own nested object so
 * that additional fields can be added per-phase independently in the future.
 * <p>
 * {@code dfs} is {@code null} when the DFS phase did not run (i.e. non-DFS search types).
 * {@code fetch} is {@code null} when no fetch phase ran (e.g. count-only requests).
 */
public record SearchShardTimingMetrics(
    @Nullable PhaseTimingMetrics dfs,
    PhaseTimingMetrics query,
    @Nullable PhaseTimingMetrics fetch
) implements Writeable, ToXContentFragment {

    /** Timing for a single search phase. */
    public record PhaseTimingMetrics(long timeInNanos) implements Writeable, ToXContentFragment {

        public PhaseTimingMetrics(StreamInput in) throws IOException {
            this(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(timeInNanos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("time_in_nanos", timeInNanos);
            return builder;
        }
    }

    public SearchShardTimingMetrics(StreamInput in) throws IOException {
        this(
            in.readOptionalWriteable(PhaseTimingMetrics::new),
            new PhaseTimingMetrics(in),
            in.readOptionalWriteable(PhaseTimingMetrics::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dfs);
        query.writeTo(out);
        out.writeOptionalWriteable(fetch);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (dfs != null) {
            builder.startObject("dfs");
            dfs.toXContent(builder, params);
            builder.endObject();
        }
        builder.startObject("query");
        query.toXContent(builder, params);
        builder.endObject();
        if (fetch != null) {
            builder.startObject("fetch");
            fetch.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }
}
