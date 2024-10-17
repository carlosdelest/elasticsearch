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
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.Set;
import java.util.regex.Pattern;

public abstract class DotPrefixValidator<RequestType> implements MappedActionFilter {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DotPrefixValidator.class);

    public static final Setting<Boolean> VALIDATE_DOT_PREFIXES = Setting.boolSetting(
        "serverless.indices.validate_dot_prefixes",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Names and patterns for indexes where no restriction should be applied.
     * Normally we would want to transition these to either system indices, or
     * to use an internal origin for the client. These are shorter-term
     * workarounds until that work can be completed.
     *
     * .elastic-connectors-* is used by enterprise search
     * .ml-* is used by ML
     * .slo-observability-* is used by Observability
     */
    private static Set<String> IGNORED_INDEX_NAMES = Set.of(
        ".elastic-connectors-v1",
        ".elastic-connectors-sync-jobs-v1",
        ".ml-state",
        ".ml-anomalies-unrelated"
    );
    private static Set<Pattern> IGNORED_INDEX_PATTERNS = Set.of(
        Pattern.compile("\\.ml-state-\\d+"),
        Pattern.compile("\\.slo-observability\\.sli-v\\d+.*"),
        Pattern.compile("\\.slo-observability\\.summary-v\\d+.*")
    );

    private final ThreadContext threadContext;
    private volatile boolean isEnabled;

    public DotPrefixValidator(ThreadContext threadContext, ClusterService clusterService) {
        this.threadContext = threadContext;
        this.isEnabled = VALIDATE_DOT_PREFIXES.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(VALIDATE_DOT_PREFIXES, this::updateEnabled);
    }

    private void updateEnabled(boolean enabled) {
        this.isEnabled = enabled;
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
        } else if (isOperator() == false) {
            for (String index : indices) {
                char c = getFirstChar(index);
                if (c == '.') {
                    deprecationLogger.critical(
                        DeprecationCategory.INDICES,
                        "dot-prefix",
                        "Index [{}] name begins with a dot (.) and will not be allowed in the future",
                        index
                    );
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    void validateIndices(@Nullable Set<String> indices) {
        if (indices != null && isOperator() == false) {
            for (String index : indices) {
                if (Strings.hasLength(index)) {
                    char c = getFirstChar(index);
                    if (c == '.') {
                        final String strippedName = stripDateMath(index);
                        if (IGNORED_INDEX_NAMES.contains(strippedName)) {
                            return;
                        }
                        if (IGNORED_INDEX_PATTERNS.stream().anyMatch(p -> p.matcher(strippedName).matches())) {
                            return;
                        }
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

    private static String stripDateMath(String index) {
        char c = index.charAt(0);
        if (c == '<') {
            assert index.charAt(index.length() - 1) == '>'
                : "expected index name with date math to start with < and end with >, how did this pass request validation? " + index;
            return index.substring(1, index.length() - 1);
        } else {
            return index;
        }
    }

    boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }
}
