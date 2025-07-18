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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomApiKeyAuthenticator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Authenticator of cloud API keys in serverless environment.
 *
 *<p>
 * This class implements the custom API key authenticator that calls the {@link CloudApiKeyService} to authenticate the cloud API key.
 * The successful authentication will return API key information (e.g. ID) and a list of assigned roles to the cloud API key.
 */
public class CloudApiKeyAuthenticator implements CustomApiKeyAuthenticator, Closeable {

    public static final String CLIENT_AUTHENTICATION_HEADER = "X-Client-Authentication";

    private final CloudApiKeyService cloudApiKeyService;
    private final ThreadPool threadPool;

    public CloudApiKeyAuthenticator(CloudApiKeyService cloudApiKeyService, ThreadPool threadPool) {
        this.cloudApiKeyService = cloudApiKeyService;
        this.threadPool = threadPool;
    }

    @Override
    public String name() {
        return "cloud API key";
    }

    /**
     * Extracts a {@link CloudApiKey} from the given secure string if it starts with the cloud API key prefix.
     */
    @Override
    public AuthenticationToken extractCredentials(@Nullable SecureString apiKeyCredentials) {
        if (apiKeyCredentials != null) {
            if (CloudApiKey.hasCloudApiKeyPrefix(apiKeyCredentials)) {
                SecureString clientCredentials = UniversalIamUtils.getHeaderValue(
                    threadPool.getThreadContext(),
                    CLIENT_AUTHENTICATION_HEADER
                );
                return new CloudApiKey(apiKeyCredentials, clientCredentials);
            }
        }
        return null;
    }

    @Override
    public void authenticate(
        @Nullable AuthenticationToken authenticationToken,
        ActionListener<AuthenticationResult<Authentication>> listener
    ) {
        if (false == authenticationToken instanceof CloudApiKey) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }

        final CloudApiKey cloudApiKey = (CloudApiKey) authenticationToken;
        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> doAuthenticate(cloudApiKey, l)));
    }

    private void doAuthenticate(CloudApiKey cloudApiKey, ActionListener<AuthenticationResult<Authentication>> listener) {
        cloudApiKeyService.authenticate(
            cloudApiKey,
            listener.delegateFailureAndWrap((l, authentication) -> l.onResponse(AuthenticationResult.success(authentication)))
        );

    }

    @Override
    public void close() throws IOException {
        cloudApiKeyService.close();
    }
}
