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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.List;
import java.util.Optional;

/**
 * A class to perform validation on Setting.Property.ServerlessPublic
 */
public class PublicSettingsValidator {

    private final ThreadContext threadContext;
    private IndexScopedSettings indexScopedSettings;

    public PublicSettingsValidator(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        this.threadContext = threadContext;
        this.indexScopedSettings = indexScopedSettings;
    }

    /**
     * Validates if a public user (no operator privileges) has settings with ServerlessPublic property only
     * It does not perform this validation if an operator privileges are set
     * @param settings - settings from the request
     * @throws IllegalArgumentException with a message indicating what settings are not allowed
     */
    public void validateSettings(Settings settings) {
        if (isOperator() == false) {
            List<String> list = Optional.ofNullable(settings).orElse(Settings.EMPTY).keySet().stream().filter(settingName -> {
                assert indexScopedSettings.get(settingName) != null : settingName + "is null";
                return indexScopedSettings.get(settingName).isServerlessPublic() == false;
            }).toList();
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

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }

}
