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

package org.elasticsearch.wipe.cli.gcp;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.wipe.cli.WipeDataOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.elasticsearch.common.Strings.format;

public class GoogleCloudStorageWipeDataOperation implements WipeDataOperation {
    private static final int MAX_BATCH_DELETE_SIZE = 1000;

    private final String bucket;
    private final Storage storage;
    private final Runnable onBatchDeleted;

    public GoogleCloudStorageWipeDataOperation(String bucket, Storage storage, Runnable onBatchDeleted) {
        this.bucket = bucket;
        this.storage = storage;
        this.onBatchDeleted = Objects.requireNonNull(onBatchDeleted);
    }

    public void deleteBlobs() throws IOException {
        long totalDeleted = 0;
        List<BlobId> batch = new ArrayList<>(MAX_BATCH_DELETE_SIZE);

        // blobs are loaded in memory in pages
        for (Blob blob : storage.list(bucket).iterateAll()) {
            batch.add(BlobId.of(bucket, blob.getName()));

            // delete blobs in batch
            if (batch.size() >= MAX_BATCH_DELETE_SIZE) {
                deleteBatch(batch);
                totalDeleted += batch.size();
                batch.clear();
                onBatchDeleted.run();
            }
        }

        // delete remaining blobs in the last batch
        if (batch.isEmpty() == false) {
            deleteBatch(batch);
            totalDeleted += batch.size();
        }

        System.out.println(format("Deleted %s blobs", totalDeleted));
    }

    // VisibleForTesting
    void deleteBatch(List<BlobId> blobIds) throws IOException {
        final List<BlobId> failedBlobs = Collections.synchronizedList(new ArrayList<>());
        try {
            StorageBatch batch = storage.batch();
            final AtomicReference<StorageException> aex = new AtomicReference<>();
            for (BlobId blobId : blobIds) {
                batch.delete(blobId)
                    .notify(
                        // callback to handle response
                        new BatchResult.Callback<>() {
                            // ignore successful deletion
                            public void success(Boolean result) {}

                            public void error(StorageException ex) {
                                if (ex.getCode() == HTTP_NOT_FOUND) {
                                    // ignore not found exception
                                    return;
                                }
                                if (failedBlobs.size() < 10) {
                                    // track up to 10 failed blob deletions for the exception message
                                    failedBlobs.add(blobId);
                                }
                                aex.set(ExceptionsHelper.useOrSuppress(aex.get(), ex));
                            }
                        }
                    );
            }
            batch.submit();

            StorageException ex = aex.get();
            if (ex != null) {
                throw ex;
            }

        } catch (Exception ex) {
            throw new IOException("Exception when deleting blobs: " + failedBlobs, ex);
        }
    }

}
