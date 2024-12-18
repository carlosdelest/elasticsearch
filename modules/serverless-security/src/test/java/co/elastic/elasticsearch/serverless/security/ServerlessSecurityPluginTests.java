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

package co.elastic.elasticsearch.serverless.security;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.INCLUDED_RESERVED_ROLES_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;

public class ServerlessSecurityPluginTests extends ESTestCase {

    private ServerlessSecurityPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new ServerlessSecurityPlugin();
    }

    public void testIncludeReservedRoleSettingIsRegistered() {
        assertThat(plugin.getSettings(), hasItem(INCLUDED_RESERVED_ROLES_SETTING));
    }

    public void testDefaultValueForIncludedReservedRoles() {
        assertThat(
            INCLUDED_RESERVED_ROLES_SETTING.get(plugin.additionalSettings()),
            contains("superuser", "remote_monitoring_agent", "remote_monitoring_collector")
        );
    }

    public void testExcludeRolesSettingIsRegistered() {
        assertThat(plugin.getSettings(), hasItem(SamlRealmSettings.EXCLUDE_ROLES.apply(SingleSpSamlRealmSettings.TYPE)));
    }
}
