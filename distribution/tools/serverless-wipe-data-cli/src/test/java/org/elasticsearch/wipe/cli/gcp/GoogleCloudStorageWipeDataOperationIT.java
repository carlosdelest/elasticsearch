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

import fixture.gcs.GoogleCloudStorageHttpFixture;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.wipe.cli.WipeDataOperation.NOOP_ON_BATCH_DELETED;
import static org.hamcrest.Matchers.notNullValue;

public class GoogleCloudStorageWipeDataOperationIT extends ESTestCase {
    private static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("test.gcs.use.fixture", "true"));
    private static final String SERVICE_ACCOUNT_CREDENTIALS = System.getProperty("test.gcs.service_account_credentials");
    private static final String BUCKET = System.getProperty("test.gcs.bucket");

    private static GoogleCloudStorageWipeDataOperation operation;
    private static Storage storage;

    @ClassRule
    public static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, BUCKET, null);

    @BeforeClass
    public static void setup() throws IOException {
        if (USE_FIXTURE) {
            storage = StorageOptions.newBuilder().setHost(fixture.getAddress()).build().getService();
        } else {
            storage = GoogleCloudStorageClientHelper.createStorage(SERVICE_ACCOUNT_CREDENTIALS);
        }

        operation = new GoogleCloudStorageWipeDataOperation(BUCKET, storage, NOOP_ON_BATCH_DELETED);

        assertThat(storage, notNullValue());
        assertThat(operation, notNullValue());
    }

    @Test
    public void deleteBlobs() throws Exception {
        int numBlobs = 10;
        createTestBlobs(BUCKET, numBlobs);
        assertEquals(numBlobs, countBlobs(BUCKET));

        operation.deleteBlobs();

        assertEquals(0, countBlobs(BUCKET));
    }

    @Test
    public void deleteLotsOfBlobs() throws Exception {
        int numBlobs = 5000;
        createTestBlobs(BUCKET, numBlobs);
        assertEquals(numBlobs, countBlobs(BUCKET));

        operation.deleteBlobs();

        assertEquals(0, countBlobs(BUCKET));
    }

    @Test
    public void deleteBlobsNotFound() throws Exception {
        int numBlobs = 10;
        List<BlobId> testBlobIds = createTestBlobs(BUCKET, numBlobs);
        assertEquals(numBlobs, countBlobs(BUCKET));

        operation.deleteBlobs();
        assertEquals(0, countBlobs(BUCKET));

        // delete the same blobs again to emulate blobs not found
        operation.deleteBatch(testBlobIds);
        assertEquals(0, countBlobs(BUCKET));
    }

    private int countBlobs(String bucket) {
        int count = 0;
        for (Blob blob : storage.list(bucket).iterateAll()) {
            count++;
        }
        return count;
    }

    private List<BlobId> createTestBlobs(String bucket, int numBlobs) {
        List<BlobId> testBlobIds = new ArrayList<>();

        for (int i = 0; i < numBlobs; i++) {
            String blobName = randomAlphaOfLengthBetween(20, 80);
            String content = randomAlphaOfLengthBetween(1, 100);
            BlobId blobId = BlobId.of(bucket, blobName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

            storage.create(blobInfo, content.getBytes());
            testBlobIds.add(blobId);
        }

        return testBlobIds;
    }

}
