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

package co.elastic.elasticsearch.api.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.Objects;

/**
 * Filter for rollover requests to disallow a request that sends conditions.
 */
public class RolloverRequestValidator implements MappedActionFilter {

    private final ThreadContext threadContext;

    public RolloverRequestValidator(ThreadContext threadContext) {
        this.threadContext = Objects.requireNonNull(threadContext, "thread context cannot be null");
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        assert request instanceof RolloverRequest : "expected only rollover requests to be validated, but it was " + request.getClass();
        var rolloverReq = (RolloverRequest) request;
        if (rolloverReq.getConditions().getConditions().isEmpty() == false && isOperator() == false) {
            throw new IllegalArgumentException("rollover with conditions is not supported in serverless mode");
        }
        chain.proceed(task, action, request, listener);
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }

    @Override
    public String actionName() {
        return RolloverAction.NAME;
    }
}
