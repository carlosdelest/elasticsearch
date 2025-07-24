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

package co.elastic.elasticsearch.serverless.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import static co.elastic.elasticsearch.serverless.security.authc.ProjectServiceAccountTokenStoreSettings.CACHE_HASH_ALGO_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectServiceAccountTokenStoreSettings.CACHE_MAX_SIZE_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectServiceAccountTokenStoreSettings.CACHE_TTL_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectServiceAccountTokenStoreSettings.SERVICE_TOKEN_HASH_SETTING_BY_ACCOUNT_ID;

public class ProjectServiceAccountTokenStore implements ServiceAccountTokenStore {
    protected final Logger logger = LogManager.getLogger(ProjectServiceAccountTokenStore.class);
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final Cache<SecureString, SecureString> slowToFastHashCache;
    private final Hasher fastHasher;

    public ProjectServiceAccountTokenStore(ClusterService clusterService, ProjectResolver projectResolver) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.fastHasher = Hasher.resolve(CACHE_HASH_ALGO_SETTING.get(clusterService.getSettings()));
        this.slowToFastHashCache = CacheBuilder.<SecureString, SecureString>builder()
            .setExpireAfterWrite(CACHE_TTL_SETTING.get(clusterService.getSettings()))
            .setMaximumWeight(CACHE_MAX_SIZE_SETTING.get(clusterService.getSettings()))
            .build();
    }

    private boolean verifyCredential(String principal, SecureString providedCredential, SecureString configuredHash) {
        SecureString fastHash = slowToFastHashCache.get(configuredHash);
        SecureString hash = fastHash != null ? fastHash : configuredHash;

        if (Hasher.verifyHash(providedCredential, hash.getChars())) {
            if (fastHash == null) {
                slowToFastHashCache.put(configuredHash, new SecureString(fastHasher.hash(providedCredential)));
            }
            logger.trace("Service account authentication successful for [{}]. Used fast hash for auth [{}]", principal, fastHash != null);
            return true;
        }
        logger.trace("Service account authentication failed for [{}]. Used fast hash for auth [{}]", principal, fastHash != null);
        return false;
    }

    @Override
    public void authenticate(ServiceAccountToken token, ActionListener<StoreAuthenticationResult> listener) {
        var configuredHash = getConfiguredHash(token.getTokenId());
        if (configuredHash == null) {
            logger.info(
                "No token hash configured for service account [{}] for project [{}]",
                token.getQualifiedName(),
                projectResolver.getProjectId()
            );
            listener.onResponse(StoreAuthenticationResult.failed(TokenInfo.TokenSource.FILE));
            return;
        }

        boolean authenticated = verifyCredential(token.principal(), token.getSecret(), configuredHash);

        listener.onResponse(
            StoreAuthenticationResult.fromBooleanResult(
                TokenInfo.TokenSource.FILE,
                authenticated

            )
        );
    }

    private SecureString getConfiguredHash(ServiceAccountToken.ServiceAccountTokenId serviceAccountTokenId) {
        var clusterState = clusterService.state();
        if (projectResolver.hasProject(clusterState) == false) {
            return null;
        }

        var setting = SERVICE_TOKEN_HASH_SETTING_BY_ACCOUNT_ID.get(serviceAccountTokenId.getAccountId());

        return projectResolver.getProjectMetadata(clusterState)
            .getSecret(setting.getConcreteSettingForNamespace(serviceAccountTokenId.getTokenName()).getKey())
            .orElse(null);
    }

    // Visible for testing
    Cache.Stats cacheStats() {
        return slowToFastHashCache.stats();
    }
}
