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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Secrets that are stored in cluster state
 *
 * <p>Cluster state secrets are initially loaded on each node, from a file on disk,
 * in the format defined by {@link org.elasticsearch.common.settings.LocallyMountedSecrets}.
 * Once the cluster is running, the master node watches the file for changes. This class
 * propagates changes in the file-based secure settings from the master node out to other
 * nodes.
 *
 * <p>Since the master node should always have settings on disk, we don't need to
 * persist this class to saved cluster state, either on disk or in the cloud. Therefore,
 * we have defined this {@link ClusterState.Custom} as a private custom object. Additionally,
 * we don't want to ever write this class's secrets out in a client response, so
 * {@link #toXContentChunked(ToXContent.Params)} returns an empty iterator.
 */
public class ClusterStateSecrets extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    /**
     * The name for this data class
     *
     * <p>This name will be used to identify this {@link org.elasticsearch.common.io.stream.NamedWriteable} in cluster
     * state. See {@link #getWriteableName()}.
     */
    public static final String TYPE = "cluster_state_secrets";

    private final Secrets settings;
    private final long version;

    public ClusterStateSecrets(long version, SecureSettings settings) {
        this.version = version;
        this.settings = new Secrets(Objects.requireNonNull(settings));
    }

    public ClusterStateSecrets(StreamInput in) throws IOException {
        this.version = in.readLong();
        this.settings = new Secrets(in);
    }

    SecureSettings getSettings() {
        return new Secrets(settings);
    }

    long getVersion() {
        return version;
    }

    @Override
    public boolean isPrivate() {
        return true;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // never render this to the user
        return Collections.emptyIterator();
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
        out.writeLong(version);
        settings.writeTo(out);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ClusterStateSecrets{[all secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateSecrets that = (ClusterStateSecrets) o;
        return version == that.version && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, version);
    }

    static class Secrets implements SecureSettings {

        final Map<String, Entry> secrets;

        Secrets(SecureSettings secureSettings) {
            secrets = new HashMap<>();
            for (String key : secureSettings.getSettingNames()) {
                try {
                    secrets.put(
                        key,
                        new Entry(getValueAsByteArray(secureSettings, key), ClusterStateSecrets.getSHA256Digest(secureSettings, key))
                    );
                } catch (IOException | GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // visible for testing
        Secrets(Map<String, Entry> settings) {
            secrets = Map.copyOf(settings);
        }

        Secrets(StreamInput in) throws IOException {
            secrets = in.readMap(StreamInput::readString, v -> new Entry(in.readByteArray(), in.readByteArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(secrets, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }

        @Override
        public boolean isLoaded() {
            return true;
        }

        @Override
        public Set<String> getSettingNames() {
            return secrets.keySet();
        }

        @Override
        public SecureString getString(String setting) {
            var value = secrets.get(setting);
            if (value == null) {
                return null;
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(value.secret);
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
            return new SecureString(Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit()));
        }

        @Override
        public InputStream getFile(String setting) {
            var value = secrets.get(setting);
            if (value == null) {
                return null;
            }
            return new ByteArrayInputStream(value.secret);
        }

        @Override
        public byte[] getSHA256Digest(String setting) {
            return secrets.get(setting).sha256Digest;
        }

        @Override
        public void close() {
            if (null != secrets && secrets.isEmpty() == false) {
                for (var entry : secrets.entrySet()) {
                    entry.setValue(null);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Secrets secrets1 = (Secrets) o;
            return Objects.equals(secrets, secrets1.secrets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(secrets);
        }
    }

    private static byte[] getValueAsByteArray(SecureSettings secureSettings, String key) throws GeneralSecurityException, IOException {
        return secureSettings.getFile(key).readAllBytes();
    }

    private static byte[] getSHA256Digest(SecureSettings secureSettings, String key) {
        try {
            return secureSettings.getSHA256Digest(key);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    record Entry(byte[] secret, byte[] sha256Digest) implements Writeable {

        Entry(StreamInput in) throws IOException {
            this(in.readByteArray(), in.readByteArray());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Arrays.equals(secret, entry.secret) && Arrays.equals(sha256Digest, entry.sha256Digest);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(secret);
            result = 31 * result + Arrays.hashCode(sha256Digest);
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(secret);
            out.writeByteArray(sha256Digest);
        }
    }
}
