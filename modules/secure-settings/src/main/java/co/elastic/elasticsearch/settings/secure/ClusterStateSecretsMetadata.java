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

package co.elastic.elasticsearch.settings.secure;

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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Non-secret metadata for cluster secrets
 *
 * <p>This class contains public-facing information for file-based settings, such as
 * a version number for the settings (which must be incremented for settings updates to
 * be applied) and any errors in processing the file. This information does not need to
 * be saved in snapshots or on disk, but it should be serialized when clients request
 * cluster state.
 */
public class ClusterStateSecretsMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    /**
     * The name for this data class
     *
     * <p>This name will be used to identify this {@link org.elasticsearch.common.io.stream.NamedWriteable} in cluster
     * state. See {@link #getWriteableName()}.
     */
    public static final String TYPE = "file_secure_settings_metadata";

    private final boolean success;
    private final long version;
    private final List<String> errorStackTrace;

    private ClusterStateSecretsMetadata(boolean success, long version, List<String> errorStackTrace) {
        this.success = success;
        this.version = version;
        this.errorStackTrace = errorStackTrace == null ? List.of() : errorStackTrace;
    }

    public ClusterStateSecretsMetadata(StreamInput in) throws IOException {
        this.success = in.readBoolean();
        this.version = in.readLong();
        this.errorStackTrace = in.readStringCollectionAsList();
    }

    public static ClusterStateSecretsMetadata createSuccessful(long version) {
        return new ClusterStateSecretsMetadata(true, version, List.of());
    }

    public static ClusterStateSecretsMetadata createError(long version, List<String> errorStackTrace) {
        return new ClusterStateSecretsMetadata(false, version, errorStackTrace);
    }

    public boolean isSuccess() {
        return success;
    }

    public long getVersion() {
        return version;
    }

    public List<String> getErrorStackTrace() {
        return errorStackTrace;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.single((builder, params) -> {
            builder.field("success", this.success);
            builder.field("version", this.version);
            if (errorStackTrace.isEmpty() == false) {
                builder.stringListField("stack_trace", errorStackTrace);
            }
            return builder;
        });
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_9_X;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeLong(version);
        out.writeStringCollection(errorStackTrace);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ClusterStateSecretsMetadata{"
            + "success="
            + success
            + ", version="
            + version
            + ", errorStackTrace="
            + errorStackTrace
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateSecretsMetadata that = (ClusterStateSecretsMetadata) o;
        return success == that.success && version == that.version && Objects.equals(errorStackTrace, that.errorStackTrace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, errorStackTrace, version);
    }
}
