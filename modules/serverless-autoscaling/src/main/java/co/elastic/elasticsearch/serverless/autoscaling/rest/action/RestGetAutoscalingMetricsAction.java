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

package co.elastic.elasticsearch.serverless.autoscaling.rest.action;

import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestGetAutoscalingMetricsAction extends BaseRestHandler {

    public static final String TIMEOUT = "timeout";

    @Override
    public String getName() {
        return "get_serverless_autoscaling_metrics";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_internal/serverless/autoscaling"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        TimeValue timeout = request.paramAsTime(TIMEOUT, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetAutoscalingMetricsAction.INSTANCE,
            new GetAutoscalingMetricsAction.Request(timeout),
            new RestToXContentListener<>(channel)
        );
    }
}
