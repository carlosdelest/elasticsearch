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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.google.cloud.storage.Storage;

import org.elasticsearch.wipe.cli.azure.AzureBlobClientHelper;
import org.elasticsearch.wipe.cli.azure.AzureBlobWipeDataOperation;
import org.elasticsearch.wipe.cli.gcp.GoogleCloudStorageClientHelper;
import org.elasticsearch.wipe.cli.gcp.GoogleCloudStorageWipeDataOperation;
import org.elasticsearch.wipe.cli.s3.S3ClientHelper;
import org.elasticsearch.wipe.cli.s3.S3WipeDataOperation;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public interface WipeDataOperation {

    Runnable NOOP_ON_BATCH_DELETED = () -> {};

    Runnable PRINT_DOT_ON_BATCH_DELETED = () -> System.out.print(".");

    void deleteBlobs() throws IOException;

    static WipeDataOperation create(Properties properties) throws IOException {
        final String type = properties.getProperty("type");
        final String client = properties.getProperty("client");

        if ("default".equals(client) == false) {
            System.err.println("warning: 'client' was [" + client + "], but 'default' is expected");
        }

        return switch (type) {
            case "s3" -> createS3Operation(properties);
            case "azure" -> createAzureOperation(properties);
            case "gcs" -> createGoogleCloudStorageOperation(properties);
            case null, default -> throw new IllegalArgumentException("'type' was [" + type + "], expected 's3', 'azure', or 'gcs'");
        };
    }

    private static S3WipeDataOperation createS3Operation(Properties properties) {
        final String endpoint = Objects.requireNonNullElse(properties.getProperty("endpoint"), Constants.S3_HOSTNAME);
        final String accessKey = Objects.requireNonNull(properties.getProperty("access_key"));
        final String secretKey = Objects.requireNonNull(properties.getProperty("secret_key"));
        final String bucket = Objects.requireNonNull(properties.getProperty("bucket"));
        final String basePath = Objects.requireNonNull(properties.getProperty("base_path"));

        AmazonS3 s3Client = S3ClientHelper.buildClient(endpoint, accessKey, secretKey);
        return new S3WipeDataOperation(s3Client, bucket, basePath, PRINT_DOT_ON_BATCH_DELETED);
    }

    private static AzureBlobWipeDataOperation createAzureOperation(Properties properties) {
        final String account = Objects.requireNonNull(properties.getProperty("account"));
        final String bucket = Objects.requireNonNull(properties.getProperty("bucket"));
        final String sasToken = Objects.requireNonNull(properties.getProperty("sas_token"));

        String endpoint = AzureBlobClientHelper.getStandardEndpoint(account);
        BlobServiceClient serviceClient = AzureBlobClientHelper.createServiceClient(endpoint, sasToken);
        BlobContainerClient blobContainerClient = serviceClient.getBlobContainerClient(bucket);

        return new AzureBlobWipeDataOperation(blobContainerClient, PRINT_DOT_ON_BATCH_DELETED);
    }

    private static GoogleCloudStorageWipeDataOperation createGoogleCloudStorageOperation(Properties properties) throws IOException {
        final String bucket = Objects.requireNonNull(properties.getProperty("bucket"));
        final String credentialsFile = Objects.requireNonNull(properties.getProperty("credentials_file"));

        Storage storage = GoogleCloudStorageClientHelper.createStorage(credentialsFile);

        return new GoogleCloudStorageWipeDataOperation(bucket, storage, PRINT_DOT_ON_BATCH_DELETED);
    }
}
