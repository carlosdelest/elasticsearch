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

import fixture.azure.AzureHttpFixture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.wipe.cli.azure.AzureBlobWipeDataOperation.MAX_BLOBS_PER_PAGE;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class AzureWipeDataIT extends ESTestCase {

    private static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("test.azure.use.fixture", "true"));

    private static final String AZURE_TEST_ENDPOINT = System.getProperty("test.azure.endpoint");
    private static final String AZURE_TEST_ACCOUNT = System.getProperty("test.azure.account");
    private static final String AZURE_TEST_ACCOUNT_KEY = System.getProperty("test.azure.account_key");
    private static final String AZURE_TEST_SAS_TOKEN = System.getProperty("test.azure.sas_token");
    private static final String AZURE_TEST_CONTAINER = System.getProperty("test.azure.container");

    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration MAX_CONNECTION_IDLE_TIME = Duration.ofSeconds(60);
    private static final int MAX_CONNECTIONS = 10;
    private static final int EVENT_LOOP_THREAD_COUNT = 1;
    private static final int PENDING_CONNECTION_QUEUE_SIZE = -1;

    private static EventLoopGroup eventLoopGroup;
    private static ConnectionProvider connectionProvider;
    private static BlobServiceClient blobServiceClient;

    @ClassRule
    public static AzureHttpFixture fixture = new AzureHttpFixture(
        AzureHttpFixture.Protocol.HTTP,
        AZURE_TEST_ACCOUNT,
        AZURE_TEST_CONTAINER,
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT)
    );

    @BeforeClass
    public static void start() {
        assertThat(AZURE_TEST_ENDPOINT, not(emptyString()));
        assertThat(AZURE_TEST_CONTAINER, not(emptyString()));

        // The only reason we use a custom netty client is keep references to the underlying resources blob client uses to properly close
        // them after tests to prevent thread leaks, since the blob client does not close it them or expose a close hook
        // Known issue: https://github.com/Azure/azure-sdk-for-java/issues/34203
        eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREAD_COUNT);
        connectionProvider = ConnectionProvider.builder("azure-sdk-connection-pool")
            .maxConnections(MAX_CONNECTIONS)
            .pendingAcquireMaxCount(PENDING_CONNECTION_QUEUE_SIZE)
            .pendingAcquireTimeout(CONNECTION_TIMEOUT)
            .maxIdleTime(MAX_CONNECTION_IDLE_TIME)
            .build();
        reactor.netty.http.client.HttpClient nettyHttpClient = reactor.netty.http.client.HttpClient.create(connectionProvider)
            .port(80)
            .wiretap(false)
            .resolver(DefaultAddressResolverGroup.INSTANCE)
            .runOn(eventLoopGroup);
        final HttpClient httpClient = new NettyAsyncHttpClientBuilder(nettyHttpClient).build();
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().httpClient(httpClient);

        // create blob service client based on configuration
        if (USE_FIXTURE) {
            // fixture expects authorization header, which requires account key
            blobServiceClient = builder.endpoint(fixture.getAddress())
                .credential(new StorageSharedKeyCredential(AZURE_TEST_ACCOUNT, AZURE_TEST_ACCOUNT_KEY))
                .buildClient();
        } else {
            if (AZURE_TEST_SAS_TOKEN.isEmpty() == false) {
                blobServiceClient = builder.endpoint(AZURE_TEST_ENDPOINT).sasToken(AZURE_TEST_SAS_TOKEN).buildClient();
            } else if (AZURE_TEST_ACCOUNT.isEmpty() == false && AZURE_TEST_ACCOUNT_KEY.isEmpty() == false) {
                blobServiceClient = builder.endpoint(AZURE_TEST_ENDPOINT)
                    .credential(new StorageSharedKeyCredential(AZURE_TEST_ACCOUNT, AZURE_TEST_ACCOUNT_KEY))
                    .buildClient();
            }
        }

        assertThat(blobServiceClient, notNullValue());
    }

    @AfterClass
    public static void close() {
        Schedulers.shutdownNow();
        connectionProvider.dispose();
        eventLoopGroup.shutdownGracefully();
    }

    @Test
    public void testDeleteLotsOfBlobs() throws IOException {
        long numBlobs = MAX_BLOBS_PER_PAGE + 1000;
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(AZURE_TEST_CONTAINER);
        createTestBlobs(containerClient, numBlobs, false);

        // verify blobs are created
        long count = containerClient.listBlobs().stream().count();
        assertEquals(numBlobs, count);

        AzureBlobWipeDataOperation operation = new AzureBlobWipeDataOperation(containerClient, null);
        operation.deleteBlobs();

        // verify blobs are deleted
        count = containerClient.listBlobs().stream().count();
        assertEquals(0L, count);
    }

    @Test
    public void testDeleteBlobsNotFound() throws IOException {
        long numBlobs = 10;
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(AZURE_TEST_CONTAINER);
        List<String> testBlobs = createTestBlobs(containerClient, numBlobs, false);
        List<String> blobUrls = testBlobs.stream().map(name -> containerClient.getBlobClient(name).getBlobUrl()).toList();

        // verify blobs are created
        long count = containerClient.listBlobs().stream().count();
        assertEquals(numBlobs, count);

        // delete and verify blobs are deleted
        AzureBlobWipeDataOperation operation = new AzureBlobWipeDataOperation(containerClient, null);
        operation.deleteBlobs();
        count = containerClient.listBlobs().stream().count();
        assertEquals(0L, count);

        // delete the same blobs again to emulate blobs not found
        BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(containerClient).buildClient();
        operation.deleteBlobBatch(blobBatchClient, blobUrls);

        // verify blobs are deleted
        count = containerClient.listBlobs().stream().count();
        assertEquals(0L, count);
    }

    @Test
    public void testDeleteBlobsWithLease() throws IOException {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(AZURE_TEST_CONTAINER);
        long numBlobs = 5;

        // create blobs with and without lease
        createTestBlobs(containerClient, numBlobs, true);
        createTestBlobs(containerClient, numBlobs, false);

        // verify blobs are created
        long count = containerClient.listBlobs().stream().count();
        assertEquals(numBlobs * 2, count);

        AzureBlobWipeDataOperation operation = new AzureBlobWipeDataOperation(containerClient, null);
        operation.deleteBlobs();

        // verify blobs are deleted
        count = containerClient.listBlobs().stream().count();
        assertEquals(0L, count);
    }

    private List<String> createTestBlobs(BlobContainerClient containerClient, long numBlobs, boolean leased) {
        List<String> blobNames = new ArrayList<>();
        for (int i = 0; i < numBlobs; i++) {
            String name = randomAlphaOfLengthBetween(20, 80);
            String content = randomAlphaOfLengthBetween(1, 100);
            BlobClient blobClient = containerClient.getBlobClient(name);
            blobClient.upload(BinaryData.fromString(content));

            // put a lease (lock) on blob
            if (leased) {
                BlobLeaseClient leaseClient = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();
                // randomized lease period, -1 being indefinite
                leaseClient.acquireLease(randomBoolean() ? -1 : randomIntBetween(15, 60));
            }

            blobNames.add(name);
        }
        return blobNames;
    }
}
