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

package co.elastic.elasticsearch.metering.stats.rest;

import co.elastic.elasticsearch.metering.action.GetMeteringStatsAction;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestGetMeteringStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_serverless_metering_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_metering/stats"), new Route(GET, "/_metering/stats/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indexOrDatastreamPatterns = Strings.splitStringByCommaToArray(request.param("name"));
        GetMeteringStatsAction.Request simpleStatsRequest = new GetMeteringStatsAction.Request(indexOrDatastreamPatterns);
        final ActionType<GetMeteringStatsAction.Response> actionType;
        if (request.header("es-secondary-authorization") == null) {
            actionType = GetMeteringStatsAction.FOR_PRIMARY_USER_INSTANCE;
        } else {
            actionType = GetMeteringStatsAction.FOR_SECONDARY_USER_INSTANCE;
        }
        return channel -> client.execute(actionType, simpleStatsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }
}
