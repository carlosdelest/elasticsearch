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

package co.elastic.elasticsearch.serverless.security.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.ApiNotAvailableException;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorPrivilegesViolation;

import java.util.Objects;

/**
 * Operator privilege rules specific for the serverless deployment.
 */
public class ServerlessOperatorOnlyRegistry implements OperatorOnlyRegistry {

    private static final Logger logger = LogManager.getLogger(ServerlessOperatorOnlyRegistry.class);

    public OperatorPrivilegesViolation check(String action, TransportRequest request) {
        return null;  // do nothing
    }

    @Override
    public OperatorPrivilegesViolation checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel) {
        try {
            Scope scope = restHandler.getServerlessScope();
            Objects.requireNonNull(
                scope,
                "Serverless scope must not be null. " + "Please report this as a bug. Request URI: [" + restRequest.uri() + "]"
            ); // upstream guarantees this is never null
            if (Scope.INTERNAL.equals(scope)) {
                throw new ApiNotAvailableException(
                    "Request for uri [{}] with method [{}] exists but is not available when running in serverless mode",
                    restRequest.uri(),
                    restRequest.method()
                );
            } else {
                assert Scope.PUBLIC.equals(scope);
                restRequest.markPathRestricted("serverless");
                logger.trace("Marked request for uri [{}] as restricted for serverless", restRequest.uri());
            }
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new ElasticsearchException(e);
        }
        return null;
    }
}
