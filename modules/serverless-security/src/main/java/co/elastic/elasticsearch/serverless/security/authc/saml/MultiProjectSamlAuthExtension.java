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

package co.elastic.elasticsearch.serverless.security.authc.saml;

import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;

import java.util.Map;

public class MultiProjectSamlAuthExtension implements SecurityExtension {
    @Override
    public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
        var clusterService = components.clusterService();
        return Map.of(
            MultiProjectSpSamlRealmSettings.TYPE,
            config -> SamlRealm.create(
                config,
                XPackPlugin.getSharedSslService(),
                components.resourceWatcherService(),
                components.roleMapper(),
                MultiProjectSamlSpConfiguration.create(components.projectResolver(), config, clusterService.getClusterSettings())
            )
        );
    }
}
