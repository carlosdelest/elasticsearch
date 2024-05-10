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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.List;
import java.util.Optional;

/**
 * An action filter that performs a validation of incoming requests with settings object.
 * The validation is preventing public users to use non-public settings.
 */
public abstract class PublicSettingsValidator<RequestType extends ActionRequest> implements MappedActionFilter {
    private final ThreadContext threadContext;
    private final IndexScopedSettings indexScopedSettings;

    public PublicSettingsValidator(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        this.threadContext = threadContext;
        this.indexScopedSettings = indexScopedSettings;
    }

    protected abstract Settings getSettingsFromRequest(RequestType request);

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
        Settings apply = getSettingsFromRequest((RequestType) request);
        validateSettings(apply);
        chain.proceed(task, action, request, listener);
    }

    /**
     * Validates if a public user (no operator privileges) has settings with ServerlessPublic property only
     * It does not perform this validation if an operator privileges are set
     *
     * @param settings - settings from the request
     * @throws IllegalArgumentException with a message indicating what settings are not allowed
     */
    void validateSettings(Settings settings) {
        if (isOperator() == false) {
            Settings normalised = normaliseSettings(settings);
            List<String> list = normalised.keySet()
                .stream()
                .filter(settingName -> indexScopedSettings.get(settingName) != null) // unknown settings will be validated later
                .filter(settingName -> indexScopedSettings.get(settingName).isServerlessPublic() == false)
                .toList();
            if (false == list.isEmpty()) {
                throw new IllegalArgumentException(
                    "Settings ["
                        + Strings.collectionToDelimitedString(list, ",")
                        + "]"
                        + " are not available when running in serverless mode"
                );
            }
        }

    }

    private static Settings normaliseSettings(Settings settings) {
        Settings toValidate = Optional.ofNullable(settings).orElse(Settings.EMPTY);
        return Settings.builder().put(toValidate).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }
}
