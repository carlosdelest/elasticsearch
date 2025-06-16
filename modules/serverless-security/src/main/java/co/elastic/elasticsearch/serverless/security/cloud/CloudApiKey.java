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

package co.elastic.elasticsearch.serverless.security.cloud;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.io.Closeable;

/**
 * Representation of a universal cloud API key used as authentication token in serverless environments.
 *
 * @param cloudApiKeyCredentials the secure string containing the cloud API key credentials.
 *                               The cloud API key credentials start with a prefix {@code essu_},
 *                               which is used to identify the key as a cloud API key.
 */
public record CloudApiKey(SecureString cloudApiKeyCredentials) implements AuthenticationToken, Closeable {

    /**
     * Prefix of the universal cloud API keys. All cloud API keys created by universal IAM service will have this prefix.
     */
    private static final String CLOUD_API_KEY_CREDENTIAL_PREFIX = "essu_";

    @Override
    public String principal() {
        return "<unauthenticated-cloud-api-key>";
    }

    @Override
    public Object credentials() {
        return cloudApiKeyCredentials;
    }

    @Override
    public void clearCredentials() {
        cloudApiKeyCredentials.close();
    }

    @Override
    public void close() {
        cloudApiKeyCredentials.close();
    }

    /**
     * Extracts a {@link CloudApiKey} from the given secure string if it starts with the cloud API key prefix.
     */
    @Nullable
    public static CloudApiKey fromApiKeyString(@Nullable SecureString apiKeyString) {
        return apiKeyString != null && hasCloudApiKeyPrefix(apiKeyString) ? new CloudApiKey(apiKeyString) : null;
    }

    private static boolean hasCloudApiKeyPrefix(SecureString apiKeyString) {
        final String rawString = apiKeyString.toString();
        return rawString.length() > CLOUD_API_KEY_CREDENTIAL_PREFIX.length()
            && rawString.regionMatches(true, 0, CLOUD_API_KEY_CREDENTIAL_PREFIX, 0, CLOUD_API_KEY_CREDENTIAL_PREFIX.length());
    }
}
