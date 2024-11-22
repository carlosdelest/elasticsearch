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

package org.elasticsearch.wipe.cli.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import org.elasticsearch.wipe.cli.WipeDataOperation;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.azure.storage.blob.models.BlobErrorCode.BLOB_NOT_FOUND;
import static com.azure.storage.blob.models.BlobErrorCode.LEASE_ID_MISSING;
import static org.elasticsearch.common.Strings.format;

public class AzureBlobWipeDataOperation implements WipeDataOperation {
    // delete up to 256 blobs per batch: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch
    private static final int MAX_BATCH_DELETE_SIZE = 256;
    // list up to 5000 blobs per page: https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources#Subheading1
    protected static final int MAX_BLOBS_PER_PAGE = 5000;

    private static final Duration TIMEOUT_LIST_BLOBS = Duration.of(10, ChronoUnit.SECONDS);
    private static final Duration TIMEOUT_BATCH_DELETE = Duration.of(15, ChronoUnit.SECONDS);

    private final BlobContainerClient containerClient;
    private final Runnable onBatchDeleted;

    public AzureBlobWipeDataOperation(BlobContainerClient containerClient, Runnable onBatchDeleted) {
        this.containerClient = containerClient;
        this.onBatchDeleted = onBatchDeleted;
    }

    public void deleteBlobs() throws IOException {
        BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(containerClient).buildClient();

        long totalDeleted = 0;
        String continuationToken = null;
        List<String> batchBlobs = new ArrayList<>();
        do {
            PagedIterable<BlobItem> blobPages = containerClient.listBlobs(
                new ListBlobsOptions().setMaxResultsPerPage(MAX_BLOBS_PER_PAGE),
                continuationToken,
                TIMEOUT_LIST_BLOBS
            );

            for (PagedResponse<BlobItem> blobPage : blobPages.iterableByPage()) {
                for (BlobItem blobItem : blobPage.getValue()) {
                    batchBlobs.add(blobItem.getName());

                    // delete blobs in batch
                    if (batchBlobs.size() == MAX_BATCH_DELETE_SIZE) {
                        deleteBlobBatch(blobBatchClient, batchBlobs);
                        totalDeleted += batchBlobs.size();
                        batchBlobs.clear();
                        if (onBatchDeleted != null) {
                            onBatchDeleted.run();
                        }
                    }
                }
                continuationToken = blobPage.getContinuationToken();
            }
        } while (continuationToken != null);

        // delete remaining blobs that didn't fill a complete batch
        if (batchBlobs.isEmpty() == false) {
            deleteBlobBatch(blobBatchClient, batchBlobs);
            totalDeleted += batchBlobs.size();
        }
        System.out.println(format("Deleted %s blobs", totalDeleted));
    }

    // VisibleForTesting
    void deleteBlobBatch(BlobBatchClient blobBatchClient, List<String> blobNames) throws IOException {
        try {
            // Submit batch delete, and do not throw for any individual blob failure. If a blob is leased, deletion will fail,
            // it will be handled in a follow-up retry delete. Assume no blob has lease for the first try, this is to avoid
            // checking for lease for every single blob
            Map<String, Response<Void>> blobResponses = submitBatchDelete(blobBatchClient, blobNames, false);
            List<String> blobsToRetry = blobResponses.entrySet()
                .stream()
                .filter(entry -> handleResponseAndCheckIfRetryable(entry.getKey(), entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

            // Submit retry batch, throw exception if any blob fails
            if (blobsToRetry.isEmpty() == false) {
                submitBatchDelete(blobBatchClient, blobsToRetry, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(format("Failed to delete blobs: %s", blobNames.stream().limit(10).toList()), e);
        }
    }

    private Map<String, Response<Void>> submitBatchDelete(
        BlobBatchClient blobBatchClient,
        List<String> blobNames,
        boolean throwOnAnyFailure
    ) {
        // The bulk exception do not indicate which individual blob request failed. We retain a mapping of blob name to
        // reference of the response to keep track of which blob failed
        BlobBatch batch = blobBatchClient.getBlobBatch();
        Map<String, Response<Void>> blobResponses = blobNames.stream()
            .collect(
                Collectors.toMap(
                    blobName -> blobName,
                    // delete snapshot as well
                    blobName -> batch.deleteBlob(
                        containerClient.getBlobClient(blobName).getBlobUrl(),
                        DeleteSnapshotsOptionType.INCLUDE,
                        null
                    )
                )
            );

        blobBatchClient.submitBatchWithResponse(batch, throwOnAnyFailure, TIMEOUT_BATCH_DELETE, Context.NONE);

        return blobResponses;
    }

    private boolean handleResponseAndCheckIfRetryable(String blobName, Response<Void> response) {
        try {
            // reading response is expected to throw if exception encountered
            Void notUsed = response.getValue();
            return false;
        } catch (BlobStorageException e) {
            if (isNotFoundException(e)) {
                // ignore not found
                return false;
            }
            if (isLeaseMissingError(e)) {
                // deletion fails if a blob is locked, we will break the lease, and mark it for retry
                new BlobLeaseClientBuilder().blobClient(containerClient.getBlobClient(blobName)).buildClient().breakLease();
                return true;
            }
            // other unhandled failure
            throw e;
        }
    }

    private boolean isNotFoundException(BlobStorageException e) {
        return BLOB_NOT_FOUND.equals(e.getErrorCode());
    }

    private boolean isLeaseMissingError(BlobStorageException e) {
        return LEASE_ID_MISSING.equals(e.getErrorCode());
    }
}
