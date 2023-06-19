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

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.root.MainRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

@ServerlessScope(Scope.PUBLIC)
public class ServerlessRestMainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/"), new Route(HEAD, "/"));
    }

    @Override
    public String getName() {
        return "main_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> client.execute(
            ServerlessMainAction.INSTANCE,
            new MainRequest(),
            new RestBuilderListener<ServerlessMainResponse>(channel) {
                @Override
                public RestResponse buildResponse(ServerlessMainResponse mainResponse, XContentBuilder builder) throws Exception {
                    return convertMainResponse(mainResponse, request, builder);
                }
            }
        );
    }

    static RestResponse convertMainResponse(ServerlessMainResponse response, RestRequest request, XContentBuilder builder)
        throws IOException {
        // Default to pretty printing, but allow ?pretty=false to disable
        if (request.hasParam("pretty") == false) {
            builder.prettyPrint().lfAtEnd();
        }
        response.toXContent(builder, request);
        return new RestResponse(RestStatus.OK, builder);
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
