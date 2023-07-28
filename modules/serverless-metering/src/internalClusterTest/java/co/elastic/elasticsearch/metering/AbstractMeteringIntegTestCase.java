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

package co.elastic.elasticsearch.metering;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractMeteringIntegTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MeteringPlugin.class, MockTransportService.TestPlugin.class);
    }

    private Settings settingsForRoles(DiscoveryNodeRole... roles) {
        return Settings.builder()
            .put(MeteringPlugin.PROJECT_ID.getKey(), "testProjectId")
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Arrays.stream(roles).map(DiscoveryNodeRole::roleName).toList())
            .build();
    }

    protected String startMasterAndIndexNode() {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE));
    }
}
