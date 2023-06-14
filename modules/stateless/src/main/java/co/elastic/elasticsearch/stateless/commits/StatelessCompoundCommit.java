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

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents a Lucene commit point with additional information required to manage this commit in the object store as well as locally. Such
 * objects are uploaded to the object store as binary blobs.
 */
public record StatelessCompoundCommit(
    ShardId shardId,
    long generation,
    long primaryTerm,
    String nodeEphemeralId,
    Map<String, BlobLocation> commitFiles
) implements Writeable {

    public static final String NAME = "stateless_commit_";

    @Override
    public String toString() {
        return "stateless_commit " + shardId + '[' + primaryTerm + "][" + generation + ']';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVLong(generation);
        out.writeVLong(primaryTerm);
        out.writeString(nodeEphemeralId);
        out.writeMap(commitFiles, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public static StatelessCompoundCommit readFromTransport(StreamInput in) throws IOException {
        return new StatelessCompoundCommit(
            new ShardId(in),
            in.readVLong(),
            in.readVLong(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, BlobLocation::readFromTransport)
        );
    }

    public static class Writer {

        private final ShardId shardId;
        private final long generation;
        private final long primaryTerm;
        private final String nodeEphemeralId;

        // Referenced blob files are files already stored in different blobs on the object store. We already
        // know the location, so we directly serialize the location. Internal files are files that
        // are going to be written in this commit file. We do not know their specific blob locations as we
        // don't know the correct offset until serializing the header of the commit. However, on the read path we
        // convert these internal files into blob locations as we can correctly calculate the offsets after
        // knowing the length of the serialized header.
        private final Map<String, BlobLocation> referencedBlobFiles = new HashMap<>();
        private final List<InternalFile> internalFiles = new ArrayList<>();

        public Writer(ShardId shardId, long generation, long primaryTerm, String nodeEphemeralId) {
            this.shardId = shardId;
            this.generation = generation;
            this.primaryTerm = primaryTerm;
            this.nodeEphemeralId = nodeEphemeralId;
        }

        public void addReferencedBlobFile(String name, BlobLocation location) {
            referencedBlobFiles.put(name, location);
        }

        public void addInternalFile(String fileName, long fileLength) {
            internalFiles.add(new InternalFile(fileName, fileLength));
        }

        public List<String> getInternalFiles() {
            return internalFiles.stream().map(InternalFile::name).collect(Collectors.toList());
        }

        public long getInternalFilesLength() {
            return internalFiles.stream().mapToLong(InternalFile::length).sum();
        }

        private record InternalFile(String name, long length) implements Writeable, ToXContentObject {

            private static final ConstructingObjectParser<InternalFile, Void> PARSER = new ConstructingObjectParser<>(
                "internal_file",
                true,
                args -> new InternalFile((String) args[0], (long) args[1])
            );
            static {
                PARSER.declareString(constructorArg(), new ParseField("name"));
                PARSER.declareLong(constructorArg(), new ParseField("length"));
            }

            private InternalFile(StreamInput streamInput) throws IOException {
                this(streamInput.readString(), streamInput.readLong());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeLong(length);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field("name", name).field("length", length).endObject();
            }
        }

        private long headerSize = -1;

        public long writeToStore(OutputStream output, Directory directory) throws IOException {
            var positionTracking = new PositionTrackingOutputStreamStreamOutput(output);
            writeHeader(positionTracking, CURRENT_VERSION);

            for (InternalFile internalFile : internalFiles) {
                try (ChecksumIndexInput input = directory.openChecksumInput(internalFile.name(), IOContext.READONCE)) {
                    Streams.copy(new InputStreamIndexInput(input, internalFile.length()), positionTracking, false);
                }
            }
            return positionTracking.position();
        }

        void writeHeader(PositionTrackingOutputStreamStreamOutput positionTracking, int version) throws IOException {
            if (version < VERSION_WITH_XCONTENT_ENCODING) {
                BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
                CodecUtil.writeHeader(new OutputStreamDataOutput(out), SHARD_COMMIT_CODEC, version);
                TransportVersion.writeVersion(TransportVersion.current(), out);
                out.writeWriteable(shardId);
                out.writeVLong(generation);
                out.writeVLong(primaryTerm);
                out.writeString(nodeEphemeralId);
                out.writeMap(
                    referencedBlobFiles,
                    StreamOutput::writeString,
                    (so, v) -> v.writeToStore(so, version >= VERSION_WITH_BLOB_LENGTH)
                );
                out.writeList(internalFiles);
                out.flush();
                // Add 8 bytes for the header size field and 4 bytes for the checksum
                headerSize = positionTracking.position() + 8 + 4;
                out.writeLong(headerSize);
                out.writeInt((int) out.getChecksum());
                out.flush();
            } else {
                writeXContentHeader(positionTracking, version);
            }
        }

        private void writeXContentHeader(PositionTrackingOutputStreamStreamOutput positionTracking, int version) throws IOException {
            assert positionTracking.position() == 0;
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), SHARD_COMMIT_CODEC, version);
            long codecSize = positionTracking.position();

            var bytesStreamOutput = new BytesStreamOutput();
            try (var b = new XContentBuilder(XContentType.SMILE.xContent(), bytesStreamOutput)) {
                b.startObject();
                {
                    shardIdXContent(shardId, b);
                    b.field("generation", generation);
                    b.field("primary_term", primaryTerm);
                    b.field("node_ephemeral_id", nodeEphemeralId);
                    b.startObject("commit_files");
                    {
                        for (Map.Entry<String, BlobLocation> e : referencedBlobFiles.entrySet()) {
                            b.field(e.getKey());
                            e.getValue().toXContent(b, ToXContent.EMPTY_PARAMS);
                        }
                    }
                    b.endObject();
                    b.startArray("internal_files");
                    {
                        for (InternalFile f : internalFiles) {
                            f.toXContent(b, ToXContent.EMPTY_PARAMS);
                        }
                    }
                    b.endArray();
                }
                b.endObject();
            }
            // Write the end marker manually, can't customize XContent to use SmileGenerator.Feature#WRITE_END_MARKER
            bytesStreamOutput.write(XContentType.SMILE.xContent().streamSeparator());
            bytesStreamOutput.flush();

            BytesReference xContentHeader = bytesStreamOutput.bytes();
            out.writeInt(xContentHeader.length());
            out.writeInt((int) out.getChecksum());
            xContentHeader.writeTo(out);
            out.writeInt((int) out.getChecksum());
            out.flush();

            headerSize = positionTracking.position();
            assert headerSize >= 0;
            assert headerSize == codecSize + 4 + 4 + xContentHeader.length() + 4;
        }

        public StatelessCompoundCommit finish(String commitFileName) {
            Map<String, BlobLocation> commitFiles = combineCommitFiles(
                commitFileName,
                primaryTerm,
                internalFiles,
                headerSize,
                referencedBlobFiles
            );

            return new StatelessCompoundCommit(shardId, generation, primaryTerm, nodeEphemeralId, Collections.unmodifiableMap(commitFiles));
        }
    }

    private static final String SHARD_COMMIT_CODEC = "stateless_commit";
    static final int VERSION_WITH_COMMIT_FILES = 0;
    static final int VERSION_WITH_BLOB_LENGTH = 1;
    static final int VERSION_WITH_XCONTENT_ENCODING = 2;
    static final int CURRENT_VERSION = VERSION_WITH_XCONTENT_ENCODING;

    public static StatelessCompoundCommit readFromStore(StreamInput in, long fileLength) throws IOException {
        try (BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(in, SHARD_COMMIT_CODEC)) {
            int version = CodecUtil.checkHeader(
                new InputStreamDataInput(input),
                SHARD_COMMIT_CODEC,
                VERSION_WITH_COMMIT_FILES,
                CURRENT_VERSION
            );
            if (version < VERSION_WITH_XCONTENT_ENCODING) {
                TransportVersion.readVersion(input);
                ShardId shardId = new ShardId(input);
                long generation = input.readVLong();
                long primaryTerm = input.readVLong();
                String nodeEphemeralId = input.readString();
                Map<String, BlobLocation> referencedBlobLocations = input.readMap(
                    StreamInput::readString,
                    (is) -> BlobLocation.readFromStore(is, version == VERSION_WITH_BLOB_LENGTH)
                );
                List<Writer.InternalFile> internalFiles = input.readList(Writer.InternalFile::new);
                long headerSize = input.readLong();
                verifyChecksum(input);
                return statelessCompoundCommit(
                    shardId,
                    generation,
                    primaryTerm,
                    nodeEphemeralId,
                    referencedBlobLocations,
                    internalFiles,
                    headerSize
                );
            } else {
                assert version == VERSION_WITH_XCONTENT_ENCODING;

                int xContentLength = input.readInt();
                verifyChecksum(input);

                byte[] bytes = new byte[xContentLength];
                input.readBytes(bytes, 0, bytes.length);
                verifyChecksum(input);

                return readXContentHeader(new BytesArray(bytes).streamInput(), fileLength);
            }
        } catch (Exception e) {
            throw new IOException("Failed to read shard commit", e);
        }
    }

    private static void verifyChecksum(BufferedChecksumStreamInput input) throws IOException {
        long actualChecksum = input.getChecksum();
        long expectedChecksum = Integer.toUnsignedLong(input.readInt());
        if (actualChecksum != expectedChecksum) {
            throw new CorruptIndexException(
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(actualChecksum),
                input.getSource()
            );
        }
    }

    private static StatelessCompoundCommit readXContentHeader(StreamInput is, long fileLength) throws IOException {
        record XContentStatelessCompoundCommit(
            ShardId shardId,
            long generation,
            long primaryTerm,
            String nodeEphemeralId,
            Map<String, BlobLocation> referencedBlobLocations,
            List<Writer.InternalFile> internalFiles
        ) {
            @SuppressWarnings("unchecked")
            private static final ConstructingObjectParser<XContentStatelessCompoundCommit, Void> PARSER = new ConstructingObjectParser<>(
                "stateless_compound_commit",
                true,
                args -> new XContentStatelessCompoundCommit(
                    (ShardId) args[0],
                    (long) args[1],
                    (long) args[2],
                    (String) args[3],
                    (Map<String, BlobLocation>) args[4],
                    (List<Writer.InternalFile>) args[5]
                )
            );
            static {
                PARSER.declareObject(constructorArg(), SHARD_ID_PARSER, new ParseField("shard_id"));
                PARSER.declareLong(constructorArg(), new ParseField("generation"));
                PARSER.declareLong(constructorArg(), new ParseField("primary_term"));
                PARSER.declareString(constructorArg(), new ParseField("node_ephemeral_id"));
                PARSER.declareObject(
                    constructorArg(),
                    (p, c) -> p.map(HashMap::new, BlobLocation::fromXContent),
                    new ParseField("commit_files")
                );
                PARSER.declareObjectArray(constructorArg(), Writer.InternalFile.PARSER, new ParseField("internal_files"));
            }
        }

        try (XContentParser parser = XContentType.SMILE.xContent().createParser(XContentParserConfiguration.EMPTY, is)) {
            XContentStatelessCompoundCommit c = XContentStatelessCompoundCommit.PARSER.parse(parser, null);

            // The XContent parser reads the stream in buffers, so we can't just count the amount of read bytes,
            // so we have to calculate the header size based on file length and the cumulative length of the internal files
            long internalFilesCount = c.internalFiles.stream().mapToLong(Writer.InternalFile::length).sum();
            long headerSize = fileLength - internalFilesCount;
            assert headerSize > 0;
            return statelessCompoundCommit(
                c.shardId,
                c.generation,
                c.primaryTerm,
                c.nodeEphemeralId,
                c.referencedBlobLocations,
                c.internalFiles,
                headerSize
            );
        }
    }

    private static StatelessCompoundCommit statelessCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        Map<String, BlobLocation> referencedBlobLocations,
        List<Writer.InternalFile> internalFiles,
        long headerSize
    ) {
        String commitFileName = NAME + generation;
        Map<String, BlobLocation> commitFiles = combineCommitFiles(
            commitFileName,
            primaryTerm,
            internalFiles,
            headerSize,
            referencedBlobLocations
        );
        return new StatelessCompoundCommit(shardId, generation, primaryTerm, nodeEphemeralId, Collections.unmodifiableMap(commitFiles));
    }

    // This method combines the pre-existing blob locations with the files internally uploaded in this commit
    // to one map of commit file locations.
    private static Map<String, BlobLocation> combineCommitFiles(
        String commitFileName,
        long primaryTerm,
        List<Writer.InternalFile> internalFiles,
        long startingOffset,
        Map<String, BlobLocation> referencedBlobFiles
    ) {
        long blobLength = internalFiles.stream().mapToLong(Writer.InternalFile::length).sum() + startingOffset;

        var commitFiles = Maps.<String, BlobLocation>newHashMapWithExpectedSize(referencedBlobFiles.size() + internalFiles.size());
        commitFiles.putAll(referencedBlobFiles);

        long currentOffset = startingOffset;
        for (Writer.InternalFile internalFile : internalFiles) {
            commitFiles.put(
                internalFile.name(),
                new BlobLocation(primaryTerm, commitFileName, blobLength, currentOffset, internalFile.length())
            );
            currentOffset += internalFile.length();
        }

        return commitFiles;
    }

    private static void shardIdXContent(ShardId shardId, XContentBuilder b) throws IOException {
        // Can't use Shard#toXContent because it loses index_uuid
        b.startObject("shard_id").field("index", shardId.getIndex()).field("id", shardId.id()).endObject();
    }

    private static final ConstructingObjectParser<ShardId, Void> SHARD_ID_PARSER = new ConstructingObjectParser<>(
        "shard_id",
        args -> new ShardId((Index) args[0], (int) args[1])
    );
    static {
        SHARD_ID_PARSER.declareObject(constructorArg(), (p, c) -> Index.fromXContent(p), new ParseField("index"));
        SHARD_ID_PARSER.declareInt(constructorArg(), new ParseField("id"));
    }
}
