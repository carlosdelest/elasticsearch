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

package co.elastic.elasticsearch.serverless.multiproject.rest.action;

import co.elastic.elasticsearch.serverless.multiproject.action.TransportGetProjectStatusAction;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetProjectStatusAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_internal/serverless/project_status/{id}"));
    }

    @Override
    public String getName() {
        return "serverless_project_status";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String projectId = request.param("id");
        return channel -> client.execute(
            TransportGetProjectStatusAction.INSTANCE,
            new TransportGetProjectStatusAction.Request(RestUtils.getMasterNodeTimeout(request), projectId),
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
