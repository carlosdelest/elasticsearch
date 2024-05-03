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

package co.elastic.elasticsearch.metering.action.utils;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

public class PersistentTaskUtils {

    public static DiscoveryNode findPersistentTaskNode(ClusterState clusterState, String taskName) {
        var persistentTaskNodeId = findPersistentTaskNodeId(clusterState, taskName);
        if (persistentTaskNodeId == null) {
            return null;
        }
        return clusterState.nodes().get(persistentTaskNodeId);
    }

    public static String findPersistentTaskNodeId(ClusterState clusterState, String taskName) {
        PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (taskMetadata == null) {
            return null;
        }
        PersistentTasksCustomMetadata.PersistentTask<?> task = taskMetadata.getTask(taskName);
        if (task == null || task.isAssigned() == false) {
            return null;
        }
        return task.getExecutorNode();
    }
}
