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

package co.elasticsearch.serverless.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.root.MainRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class ServerlessTransportMainAction extends HandledTransportAction<MainRequest, ServerlessMainResponse> {

    private final String nodeName;
    private final ClusterService clusterService;

    @Inject
    public ServerlessTransportMainAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(ServerlessMainAction.NAME, transportService, actionFilters, MainRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MainRequest request, ActionListener<ServerlessMainResponse> listener) {
        ClusterState clusterState = clusterService.state();
        listener.onResponse(
            new ServerlessMainResponse(nodeName, clusterState.getClusterName(), clusterState.metadata().clusterUUID(), Build.CURRENT)
        );
    }
}
