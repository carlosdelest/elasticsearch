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

import co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin;
import co.elastic.elasticsearch.serverless.security.cloud.CloudApiKeyAuthenticator;
import co.elastic.elasticsearch.serverless.security.cloud.CloudApiKeyService;
import co.elastic.elasticsearch.serverless.security.cloud.ClusterSettingsProjectInfoSupplier;
import co.elastic.elasticsearch.serverless.security.cloud.UniversalIamClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomApiKeyAuthenticator;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin.UNIVERSAL_IAM_SERVICE_URL_SETTING;

public class ServerlessSecurityExtension implements SecurityExtension {
    private static final Logger logger = LoggerFactory.getLogger(ServerlessSecurityExtension.class);
    ServerlessSecurityPlugin plugin;

    public ServerlessSecurityExtension() {}

    public ServerlessSecurityExtension(ServerlessSecurityPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public ServiceAccountTokenStore getServiceAccountTokenStore(SecurityComponents components) {
        if (components.projectResolver().supportsMultipleProjects()) {
            return new ProjectServiceAccountTokenStore(components.clusterService(), components.projectResolver());
        }
        return null;
    }

    @Override
    public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
        ensureSinglePerProjectFileRealmConfigured(components.settings());

        return Map.of(
            MultiProjectSpSamlRealmSettings.TYPE,
            config -> SamlRealm.create(
                config,
                XPackPlugin.getSharedSslService(),
                components.resourceWatcherService(),
                components.roleMapper(),
                MultiProjectSamlSpConfiguration.create(
                    components.projectResolver(),
                    config,
                    components.clusterService().getClusterSettings()
                )
            ),
            ProjectFileSettingsRealmSettings.TYPE,
            config -> new ProjectFileSettingsRealm(config, components.projectResolver(), components.clusterService())
        );
    }

    @Override
    public CustomApiKeyAuthenticator getCustomApiKeyAuthenticator(SecurityComponents components) {
        if (components.projectResolver().supportsMultipleProjects()) {
            // TODO add multi-project support
            return null;
        }
        final Settings settings = components.settings();
        if (UNIVERSAL_IAM_SERVICE_URL_SETTING.exists(settings)) {
            return new CloudApiKeyAuthenticator(
                new CloudApiKeyService(
                    Node.NODE_NAME_SETTING.get(settings),
                    new UniversalIamClient(settings),
                    new ClusterSettingsProjectInfoSupplier(settings)
                )
            );
        }
        return null;
    }

    private void ensureSinglePerProjectFileRealmConfigured(Settings settings) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmsSettings = RealmSettings.getRealmSettings(settings);

        var projectFileRealms = realmsSettings.keySet()
            .stream()
            .filter(identifier -> identifier.getType().equals(ProjectFileSettingsRealmSettings.TYPE))
            .toList();

        if (projectFileRealms.size() > 1) {
            throw new IllegalArgumentException(
                "Multiple ["
                    + ProjectFileSettingsRealmSettings.TYPE
                    + "] realms are configured: "
                    + projectFileRealms.stream().sorted().toList()
                    + ". Only one such realm can be configured."
            );
        }
    }
}
