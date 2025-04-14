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

package co.elastic.elasticsearch.serverless.crossproject.rest.action;

import co.elastic.elasticsearch.serverless.crossproject.action.TransportGetProjectTagsAction;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.PUBLIC)
public class RestGetProjectTagsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_project/tags"));
    }

    @Override
    public String getName() {
        return "serverless_project_tags";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(
            TransportGetProjectTagsAction.INSTANCE,
            new TransportGetProjectTagsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
