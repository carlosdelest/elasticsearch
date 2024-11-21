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

package co.elastic.elasticsearch.metering.usagereports.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Objects;

/**
 * A cluster state entry that contains the last timeframe for which we successfully collected and transmitted sampled metrics.
 */
public final class SampledMetricsMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "metering-samples-metadata";

    private final Instant committedTimestamp;

    public SampledMetricsMetadata(Instant committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public SampledMetricsMetadata(StreamInput in) throws IOException {
        this.committedTimestamp = in.readInstant();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInstant(committedTimestamp);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single((builder, params) -> {
            builder.field("committed_timestamp", committedTimestamp.getEpochSecond());
            return builder;
        });
    }

    public Instant getCommittedTimestamp() {
        return committedTimestamp;
    }

    public static SampledMetricsMetadata getFromClusterState(ClusterState clusterState) {
        return clusterState.custom(SampledMetricsMetadata.TYPE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampledMetricsMetadata that = (SampledMetricsMetadata) o;
        return Objects.equals(committedTimestamp, that.committedTimestamp);
    }

    @Override
    public String toString() {
        return "SampledMetricsMetadata{" + "committedTimestamp=" + committedTimestamp + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(committedTimestamp);
    }
}
