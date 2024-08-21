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
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.Set;

public abstract class DotPrefixValidator<RequestType> implements MappedActionFilter {
    public static final Setting<Boolean> VALIDATE_DOT_PREFIXES = Setting.boolSetting(
        "serverless.indices.validate_dot_prefixes",
        false,
        Setting.Property.NodeScope
    );

    private final ThreadContext threadContext;
    private final boolean isEnabled;

    public DotPrefixValidator(ThreadContext threadContext, ClusterService clusterService) {
        this.threadContext = threadContext;
        this.isEnabled = VALIDATE_DOT_PREFIXES.get(clusterService.getSettings());
    }

    protected abstract Set<String> getIndicesFromRequest(RequestType request);

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        Set<String> indices = getIndicesFromRequest((RequestType) request);
        if (isEnabled) {
            validateIndices(indices);
        }
        chain.proceed(task, action, request, listener);
    }

    void validateIndices(@Nullable Set<String> indices) {
        if (indices != null && isOperator() == false) {
            for (String index : indices) {
                if (Strings.hasLength(index)) {
                    char c = getFirstChar(index);
                    if (c == '.') {
                        throw new IllegalArgumentException("Index [" + index + "] name beginning with a dot (.) is not allowed");
                    }
                }
            }
        }
    }

    private static char getFirstChar(String index) {
        char c = index.charAt(0);
        if (c == '<') {
            // Date-math is being used for the index, we need to
            // consider it by stripping the first '<' before we
            // check for a dot-prefix
            String strippedLeading = index.substring(1);
            if (Strings.hasLength(strippedLeading)) {
                c = strippedLeading.charAt(0);
            }
        }
        return c;
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }
}
