/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.cluster.coordination.stateless.HeartbeatStore;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.function.Supplier;

public class StatelessHeartbeatStore implements HeartbeatStore {
    private static final String HEARTBEAT_BLOB = "heartbeat";
    private final Supplier<BlobContainer> heartbeatBlobContainerSupplier;
    private final ThreadPool threadPool;

    public StatelessHeartbeatStore(Supplier<BlobContainer> heartbeatBlobContainerSupplier, ThreadPool threadPool) {
        this.heartbeatBlobContainerSupplier = heartbeatBlobContainerSupplier;
        this.threadPool = threadPool;
    }

    @Override
    public void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
        threadPool.executor(getExecutor())
            .execute(
                ActionRunnable.run(
                    listener,
                    () -> getHeartbeatBlobContainer().writeMetadataBlob(HEARTBEAT_BLOB, false, true, out -> serialize(newHeartbeat, out))
                )
            );
    }

    @Override
    public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
        threadPool.executor(getExecutor()).execute(ActionRunnable.supply(listener, () -> {
            try (InputStream inputStream = getHeartbeatBlobContainer().readBlob(HEARTBEAT_BLOB)) {
                return deserialize(inputStream);
            } catch (NoSuchFileException e) {
                return null;
            }
        }));
    }

    private BlobContainer getHeartbeatBlobContainer() {
        return heartbeatBlobContainerSupplier.get();
    }

    protected String getExecutor() {
        return ThreadPool.Names.SNAPSHOT_META;
    }

    private long serialize(Heartbeat heartbeat, OutputStream output) throws IOException {
        var positionTrackingOutput = new PositionTrackingOutputStreamStreamOutput(output);
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTrackingOutput);
        heartbeat.writeTo(out);
        out.writeInt((int) out.getChecksum());
        out.flush();

        return positionTrackingOutput.position();
    }

    private Heartbeat deserialize(InputStream inputStream) throws IOException {
        try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(new InputStreamStreamInput(inputStream), HEARTBEAT_BLOB)) {
            var heartbeat = new Heartbeat(in);

            long expectedChecksum = in.getChecksum();
            long readChecksum = Integer.toUnsignedLong(in.readInt());
            if (readChecksum != expectedChecksum) {
                throw new IllegalStateException(
                    "checksum verification failed - expected: 0x"
                        + Long.toHexString(expectedChecksum)
                        + ", got: 0x"
                        + Long.toHexString(readChecksum)
                );
            }

            return heartbeat;
        }
    }
}
