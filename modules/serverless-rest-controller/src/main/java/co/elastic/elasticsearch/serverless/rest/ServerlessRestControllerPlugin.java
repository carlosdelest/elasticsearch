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
package co.elastic.elasticsearch.serverless.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.usage.UsageService;

import java.util.function.UnaryOperator;

public class ServerlessRestControllerPlugin extends Plugin implements RestServerActionPlugin {

    @Override
    public RestController getRestController(
        UnaryOperator<RestHandler> handlerWrapper,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        Tracer tracer
    ) {
        return new ServerlessRestController(handlerWrapper, client, circuitBreakerService, usageService, tracer);
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerInterceptor(ThreadContext threadContext) {
        return null;
    }
}
