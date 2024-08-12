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

package co.elastic.elasticsearch.serverless.restroot;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.root.MainRequest;
import org.elasticsearch.rest.root.MainResponse;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class ServerlessTransportMainAction extends TransportAction<MainRequest, MainResponse> {

    private static final String VERSION = "8.11.0";
    private static final String SERVERLESS_NAME = "serverless";
    private static final String LUCENE_VERSION = "9.7.0";
    private static final Build BUILD = new Build(
        SERVERLESS_NAME,
        Build.Type.DOCKER,
        "00000000",
        "2023-10-31",
        VERSION,
        null,
        false,
        VERSION,
        VERSION,
        // the display string is not used by the main response, so it does not matter here
        ""
    );

    private final ClusterService clusterService;
    private final ClusterName projectId;

    @Inject
    public ServerlessTransportMainAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Environment env
    ) {
        super(MainRestPlugin.MAIN_ACTION.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.projectId = new ClusterName(ServerlessSharedSettings.PROJECT_ID.get(env.settings()));
    }

    @Override
    protected void doExecute(Task task, MainRequest request, ActionListener<MainResponse> listener) {
        var clusterState = clusterService.state();
        listener.onResponse(new MainResponse(SERVERLESS_NAME, LUCENE_VERSION, projectId, clusterState.metadata().clusterUUID(), BUILD));
    }
}
