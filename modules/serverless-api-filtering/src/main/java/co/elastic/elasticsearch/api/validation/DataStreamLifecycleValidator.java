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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

/**
 * An action filter that performs a validation of incoming requests with a data stream lifecycle object.
 * The validation is preventing non-operator users to disable the lifecycle of a data stream.
 */
public abstract class DataStreamLifecycleValidator<RequestType> implements MappedActionFilter {
    private final ThreadContext threadContext;

    public DataStreamLifecycleValidator(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    protected abstract DataStreamLifecycle getLifecycleFromRequest(RequestType request);

    @Nullable
    protected static DataStreamLifecycle fromTemplate(@Nullable Template template) {
        return template == null ? null : template.lifecycle();
    }

    @Nullable
    protected static DataStreamLifecycle fromIndexTemplateRequest(@Nullable TransportPutComposableIndexTemplateAction.Request request) {
        return request == null ? null : fromTemplate(request.indexTemplate().template());
    }

    @Override
    public int order() {
        return 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        DataStreamLifecycle lifecycle = getLifecycleFromRequest((RequestType) request);
        validateLifecycle(lifecycle);
        chain.proceed(task, action, request, listener);
    }

    /**
     * Validates if a public user (no operator privileges) is trying to disable the lifecycle of a data stream or a template.
     * It does not perform this validation if operator privileges are set.
     *
     * @param lifecycle - lifecycle from the request
     * @throws IllegalArgumentException with a message indicating that the `enabled=false` needs to be removed.
     */
    void validateLifecycle(@Nullable DataStreamLifecycle lifecycle) {
        if (isOperator() == false) {
            if (lifecycle != null && lifecycle.isEnabled() == false) {
                throw new IllegalArgumentException("Data stream lifecycle cannot be disabled in serverless, please remove 'enabled=false'");
            }
        }
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }
}
