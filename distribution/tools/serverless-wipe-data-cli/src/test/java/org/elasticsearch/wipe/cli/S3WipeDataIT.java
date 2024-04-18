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

import fixture.s3.S3HttpFixture;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class S3WipeDataIT extends ESTestCase {

    private static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    @ClassRule
    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE);

    private AmazonS3 s3Client = null;

    private AmazonS3 client() {
        if (s3Client == null) {
            final String endpoint = USE_FIXTURE ? s3Fixture.getAddress() : System.getProperty("test.s3.endpoint");
            assertThat(endpoint, not(emptyString()));
            final String accessKey = System.getProperty("test.s3.account");
            final String secretKey = System.getProperty("test.s3.key");
            assertThat(accessKey, not(emptyString()));
            assertThat(secretKey, not(emptyString()));

            s3Client = S3ClientHelper.buildClient(endpoint, accessKey, secretKey);
        }
        return s3Client;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (s3Client != null) {
            s3Client.shutdown(); // avoid thread leaks
        }
    }

    public void testDeleteBlobs() throws IOException {
        final String bucket = System.getProperty("test.s3.bucket");
        final String path = System.getProperty("test.s3.base");

        assertThat(bucket, not(emptyString()));
        assertThat(path, not(emptyString()));

        AmazonS3 s3Client = client();

        List<String> blobNames;

        // clean up outside the common prefix, before the test, so that the test is repeatable
        WipeDataOperation cleanupOutside = new WipeDataOperation(
            s3Client,
            bucket,
            path + "/outsideCommonPrefix",
            WipeDataOperation.NOOP_ON_BATCH_DELETED
        );
        cleanupOutside.deleteBlobs();

        // sanity check: storage is empty start
        blobNames = listObjects(s3Client, bucket, path);
        assertThat(blobNames, empty());

        final String commonPrefix = "prefix";

        // put some objects
        for (int i = 0; i < 10; i++) {
            String s = randomAlphaOfLengthBetween(20, 80);
            s3Client.putObject(bucket, path + "/" + commonPrefix + "_" + i + "/" + s, "some_content");
        }
        s3Client.putObject(bucket, path + "/outsideCommonPrefix", "some_content");

        // and verify that they exist
        blobNames = listObjects(s3Client, bucket, path);
        assertThat(blobNames, hasSize(10 + 1)); // 10 in the common prefix and one outside

        // delete everything in the common prefix
        WipeDataOperation operation = new WipeDataOperation(
            s3Client,
            bucket,
            path + "/" + commonPrefix,
            WipeDataOperation.NOOP_ON_BATCH_DELETED
        );
        operation.deleteBlobs();

        // everything in the common prefix should be gone, but nothing outside the common prefix
        blobNames = listObjects(s3Client, bucket, path);
        assertThat(blobNames, contains(path + "/outsideCommonPrefix")); // n.b. contains(...) is 'these and only these items, in order'
        assertThat(blobNames, hasSize(1)); // as a security blanket if you don't believe me about contains(...)
    }

    private static List<String> listObjects(AmazonS3 s3Client, String bucket, String path) {
        final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(path);
        final ObjectListing list = s3Client.listObjects(listObjectsRequest);
        return list.getObjectSummaries().stream().map(S3ObjectSummary::getKey).toList();
    }
}
