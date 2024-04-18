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

package org.elasticsearch.wipe.cli;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.Strings.format;

public class WipeDataOperation {

    public static final Runnable NOOP_ON_BATCH_DELETED = () -> {};

    private static final int MAX_BULK_DELETES = 1000;

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String keyPrefix;
    private final Runnable onBatchDeleted;

    public WipeDataOperation(AmazonS3 s3Client, String bucketName, String keyPrefix, Runnable onBatchDeleted) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.keyPrefix = keyPrefix;
        this.onBatchDeleted = onBatchDeleted;
    }

    public void deleteBlobs() throws IOException {
        ObjectListing prevListing = null;
        while (true) {
            final ObjectListing list;
            if (prevListing != null) {
                final var listNextBatchOfObjectsRequest = new ListNextBatchOfObjectsRequest(prevListing);
                list = s3Client.listNextBatchOfObjects(listNextBatchOfObjectsRequest);
            } else {
                final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                listObjectsRequest.setBucketName(bucketName);
                listObjectsRequest.setPrefix(keyPrefix);
                list = s3Client.listObjects(listObjectsRequest);
            }
            final Iterator<String> blobNameIterator = list.getObjectSummaries().stream().map(S3ObjectSummary::getKey).iterator();
            if (list.isTruncated()) {
                deleteBlobsIgnoringIfNotExists(blobNameIterator);
                prevListing = list;
            } else {
                deleteBlobsIgnoringIfNotExists(Iterators.concat(blobNameIterator, Iterators.single(keyPrefix)));
                break;
            }
        }
    }

    private void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        if (blobNames.hasNext() == false) {
            return;
        }

        final List<String> partition = new ArrayList<>();
        try {
            // S3 API only allows 1k blobs per delete, so we split up the given blobs into requests of max. 1k deletes
            final AtomicReference<Exception> aex = new AtomicReference<>();

            blobNames.forEachRemaining(key -> {
                partition.add(key);
                if (partition.size() == MAX_BULK_DELETES) {
                    deletePartition(partition, aex);
                    partition.clear();
                }
            });
            if (partition.isEmpty() == false) {
                deletePartition(partition, aex);
            }

            if (aex.get() != null) {
                throw aex.get();
            }
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs " + partition.stream().limit(10).toList(), e);
        }
    }

    private void deletePartition(List<String> partition, AtomicReference<Exception> aex) {
        try {
            s3Client.deleteObjects(bulkDelete(partition));
            this.onBatchDeleted.run();
        } catch (MultiObjectDeleteException e) {
            // We are sending quiet mode requests, so we can't use the deleted keys entry on the exception and instead
            // first remove all keys that were sent in the request and then add back those that ran into an exception.
            System.err.println(
                format(
                    "Failed to delete some blobs %s",
                    e.getErrors().stream().map(err -> "[" + err.getKey() + "][" + err.getCode() + "][" + err.getMessage() + "]").toList()
                )
            );
            e.printStackTrace();
            aex.set(ExceptionsHelper.useOrSuppress(aex.get(), e));
        } catch (AmazonClientException e) {
            // The AWS client threw any unexpected exception and did not execute the request at all, so we do not
            // remove any keys from the outstanding deletes set.
            aex.set(ExceptionsHelper.useOrSuppress(aex.get(), e));
        }
    }

    private DeleteObjectsRequest bulkDelete(List<String> blobs) {
        return new DeleteObjectsRequest(bucketName).withKeys(blobs.toArray(Strings.EMPTY_ARRAY)).withQuiet(true);
    }
}
