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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;

/**
 * Container for per-shard timing metrics collected across all shards of a search request.
 * Mirrors {@link SearchProfileResults} in structure — maps a composite shard id to
 * {@link SearchShardTimingMetrics} — but is much lighter weight (no Lucene instrumentation).
 */
public final class SearchTimingMetricsResults implements Writeable, ToXContentFragment {

    public static final String TIMING_METRICS_FIELD = "timing_metrics";
    public static final String SHARDS_FIELD = "shards";

    private final Map<String, SearchShardTimingMetrics> shardResults;

    public SearchTimingMetricsResults(Map<String, SearchShardTimingMetrics> shardResults) {
        this.shardResults = Collections.unmodifiableMap(shardResults);
    }

    public SearchTimingMetricsResults(StreamInput in) throws IOException {
        shardResults = in.readMap(SearchShardTimingMetrics::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(shardResults, StreamOutput::writeWriteable);
    }

    public Map<String, SearchShardTimingMetrics> getShardResults() {
        return shardResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TIMING_METRICS_FIELD).startArray(SHARDS_FIELD);
        // Sort keys so that output is stable across calls
        TreeSet<String> sortedKeys = new TreeSet<>(shardResults.keySet());
        for (String key : sortedKeys) {
            builder.startObject();
            builder.field(SearchProfileResults.ID_FIELD, key);
            shardResults.get(key).toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray().endObject();
        return builder;
    }
}
