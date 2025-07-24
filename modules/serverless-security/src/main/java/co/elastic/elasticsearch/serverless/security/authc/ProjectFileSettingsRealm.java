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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.CACHE_HASH_ALGO_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.CACHE_MAX_SIZE_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.CACHE_TTL_SETTING;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.HASH_SETTING_BY_USER;
import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.ROLES_SETTING_BY_USER;

public class ProjectFileSettingsRealm extends Realm {

    private final Cache<SecureString, SecureString> slowToFastHashCache;
    private final Hasher fastHasher;
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final Map<String, String[]> configuredRoles;
    private final Map<String, String> hashSettingKeyByPrincipal;

    public ProjectFileSettingsRealm(RealmConfig config, ProjectResolver projectResolver, ClusterService clusterService) {
        super(config);
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;

        fastHasher = Hasher.resolve(this.config.getSetting(CACHE_HASH_ALGO_SETTING));
        slowToFastHashCache = CacheBuilder.<SecureString, SecureString>builder()
            .setExpireAfterWrite(this.config.getSetting(CACHE_TTL_SETTING))
            .setMaximumWeight(this.config.getSetting(CACHE_MAX_SIZE_SETTING))
            .build();

        this.configuredRoles = getConfiguredRoles();
        this.hashSettingKeyByPrincipal = HASH_SETTING_BY_USER.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getConcreteSettingForNamespace(config.name()).getKey()));
    }

    private Map<String, String[]> getConfiguredRoles() {
        return ROLES_SETTING_BY_USER.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            Setting<List<String>> concreteRoleSetting = entry.getValue().getConcreteSettingForNamespace(config.name());
            return concreteRoleSetting.get(clusterService.getSettings()).toArray(String[]::new);
        }));
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return UsernamePasswordToken.extractToken(context);
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) token;

        if (ROLES_SETTING_BY_USER.containsKey(token.principal()) == false) {
            logger.trace("Attempt to authenticate unknown principal [{}]", token.principal());
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }

        SecureString configuredHash = getConfiguredHash(token.principal());
        if (configuredHash == null) {
            logger.info(
                "No hash secrets configured for project_file_settings realm [{}] for project [{}] and principal [{}]",
                name(),
                projectResolver.getProjectId(),
                token.principal()
            );
            handleAuthFailure(usernamePasswordToken, listener);
            return;
        }

        authenticateWithCache(configuredHash, usernamePasswordToken, listener);

    }

    private SecureString getConfiguredHash(String principal) {
        ClusterState clusterState = clusterService.state();
        if (projectResolver.hasProject(clusterState) == false) {
            return null;
        }
        return projectResolver.getProjectMetadata(clusterState).getSecret(hashSettingKeyByPrincipal.get(principal)).orElse(null);
    }

    private void authenticateWithCache(
        SecureString configuredHash,
        UsernamePasswordToken token,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        SecureString fastHash = slowToFastHashCache.get(configuredHash);
        SecureString hash = fastHash != null ? fastHash : configuredHash;

        if (Hasher.verifyHash(token.credentials(), hash.getChars())) {
            logger.trace(
                "Password authentication successful for principal [{}] in project [{}]. Cache used: [{}]",
                token.principal(),
                projectResolver.getProjectId(),
                fastHash != null
            );

            if (fastHash == null) {
                slowToFastHashCache.put(configuredHash, new SecureString(fastHasher.hash(token.credentials())));
            }

            listener.onResponse(
                AuthenticationResult.success(new User(token.principal(), configuredRoles.getOrDefault(token.principal(), new String[0])))
            );
            return;
        }

        handleAuthFailure(token, listener);
    }

    private void handleAuthFailure(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
        logger.trace("Password authentication failed for [{}]", token.principal());
        listener.onResponse(AuthenticationResult.unsuccessful("Password authentication failed for " + token.principal(), null));
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> {
            if (slowToFastHashCache != null) {
                Cache.Stats cacheStats = slowToFastHashCache.stats();
                stats.put(
                    "cache",
                    Map.of(
                        "size",
                        slowToFastHashCache.count(),
                        "evictions",
                        cacheStats.getEvictions(),
                        "hits",
                        cacheStats.getHits(),
                        "misses",
                        cacheStats.getMisses()
                    )
                );
            }
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        // run_as and delegated authorization not supported for project file settings realm
        listener.onResponse(null);
    }
}
