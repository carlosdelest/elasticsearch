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

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public abstract class DotPrefixValidator<RequestType> implements MappedActionFilter {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DotPrefixValidator.class);

    public static final Setting<Boolean> VALIDATE_DOT_PREFIXES = Setting.boolSetting(
        "serverless.indices.validate_dot_prefixes",
        true,
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
    public static Setting<List<String>> IGNORED_INDEX_PATTERNS_SETTING = Setting.stringListSetting(
        "serverless.indices.validate_ignored_dot_patterns",
        List.of(
            "\\.ml-state-\\d+",
            "\\.slo-observability\\.sli-v\\d+.*",
            "\\.slo-observability\\.summary-v\\d+.*",
            "\\.entities\\.v\\d+\\.latest\\..*"
        ),
        (patternList) -> patternList.forEach(pattern -> {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("invalid dot validation exception pattern: [" + pattern + "]", e);
            }
        }),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadContext threadContext;
    private volatile boolean isEnabled;
    private volatile Set<Pattern> ignoredIndexPatterns;

    public DotPrefixValidator(ThreadContext threadContext, ClusterService clusterService) {
        this.threadContext = threadContext;
        this.isEnabled = VALIDATE_DOT_PREFIXES.get(clusterService.getSettings());
        this.ignoredIndexPatterns = IGNORED_INDEX_PATTERNS_SETTING.get(clusterService.getSettings())
            .stream()
            .map(Pattern::compile)
            .collect(Collectors.toSet());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(VALIDATE_DOT_PREFIXES, this::updateEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(IGNORED_INDEX_PATTERNS_SETTING, this::updateIgnoredIndexPatterns);
    }

    private void updateEnabled(boolean enabled) {
        this.isEnabled = enabled;
    }

    private void updateIgnoredIndexPatterns(List<String> patterns) {
        this.ignoredIndexPatterns = patterns.stream().map(Pattern::compile).collect(Collectors.toSet());
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
                        if (this.ignoredIndexPatterns.stream().anyMatch(p -> p.matcher(strippedName).matches())) {
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
