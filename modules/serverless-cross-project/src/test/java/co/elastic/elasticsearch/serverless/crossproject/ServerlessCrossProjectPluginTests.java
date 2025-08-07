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

package co.elastic.elasticsearch.serverless.crossproject;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;

import static co.elastic.elasticsearch.serverless.crossproject.ServerlessCrossProjectPlugin.CROSS_PROJECT_ENABLED;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.hamcrest.Matchers.is;

public class ServerlessCrossProjectPluginTests extends ESTestCase {

    public void testNoRemoteClusterSettingsAddedIfCPSisDisabled() throws IOException {
        checkAdditionalSettingsForPluginInputSettings(Settings.EMPTY, Settings.EMPTY);
    }

    public void testNoRemoteClusterSettingsAddedIfCPSisEnabledOnNonSearchNode() throws IOException {
        final var roleNames = new HashSet<>(DiscoveryNodeRole.roleNames());
        roleNames.remove(DiscoveryNodeRole.SEARCH_ROLE.roleName());
        checkAdditionalSettingsForPluginInputSettings(
            Settings.builder().put(CROSS_PROJECT_ENABLED.getKey(), true).putList("node.roles", randomSubsetOf(roleNames)).build(),
            Settings.EMPTY
        );
    }

    public void testPluginEnablesRemoteClusterServer() throws IOException {
        checkAdditionalSettingsForPluginInputSettings(
            Settings.builder().put(CROSS_PROJECT_ENABLED.getKey(), true).putList("node.roles", "search").build(),
            Settings.builder().put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true).build()
        );
    }

    private static void checkAdditionalSettingsForPluginInputSettings(Settings settings, Settings expectedAdditionalSettings)
        throws IOException {
        try (var plugin = new ServerlessCrossProjectPlugin(settings)) {
            assertThat(plugin.additionalSettings(), is(expectedAdditionalSettings));
        }
    }
}
