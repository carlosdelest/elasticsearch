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

package org.elasticsearch.server.cli;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;

import java.util.List;

import static java.lang.Math.min;

/**
 * Auto heap configuration for serverless.
 */
public class ServerlessMachineDependentHeap extends MachineDependentHeap {

    // TODO: expose these from the super class
    private static final long GB = 1024L * 1024L * 1024L;
    private static final long MAX_HEAP = GB * 31;

    @Override
    protected int getHeapSizeMb(Settings nodeSettings, MachineNodeRole role, long availableMemory) {
        ProjectType projectType = ServerlessSharedSettings.PROJECT_TYPE.get(nodeSettings);

        /*
         * Vector search data nodes require more file system cache.
         * Heap is computed as 50% for nodes with less than 8GB system memory.
         * For nodes bigger or equal to 8Gb we use 25% of available memory on
         * search tier nodes, and 50% of available memory on index tier nodes,
         * up to the normal max heap.
         */
        if (projectType == ProjectType.ELASTICSEARCH_VECTOR && role == MachineNodeRole.DATA) {
            List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(nodeSettings);
            long threshold = 8 * GB;

            final double factor;
            if (availableMemory < threshold || roles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
                factor = 0.5;
            } else {
                factor = 0.25;
            }
            return mb(min((long) (availableMemory * factor), MAX_HEAP));
        }
        return super.getHeapSizeMb(nodeSettings, role, availableMemory);
    }
}
