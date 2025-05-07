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

import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;

import java.util.Map;

public class ServerlessSecurityExtension implements SecurityExtension {
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
}
