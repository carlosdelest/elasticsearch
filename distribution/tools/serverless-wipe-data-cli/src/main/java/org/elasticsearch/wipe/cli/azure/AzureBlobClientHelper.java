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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import static org.elasticsearch.core.Strings.format;

public final class AzureBlobClientHelper {

    private AzureBlobClientHelper() {
        // utility class
    }

    public static BlobServiceClient createServiceClient(String endpoint, String sasToken) {
        return new BlobServiceClientBuilder().endpoint(endpoint).sasToken(sasToken).buildClient();
    }

    public static String getStandardEndpoint(String account) {
        // https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#standard-endpoints
        return format("https://%s.blob.core.windows.net/", account);
    }
}
