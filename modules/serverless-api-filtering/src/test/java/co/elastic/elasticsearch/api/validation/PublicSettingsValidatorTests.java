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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;

public class PublicSettingsValidatorTests extends ESTestCase {

    public void testPublicSettingAllowedForOperator() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            // set operator privileges
            THREAD_CONTEXT.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);

            PublicSettingsValidator publicSettingsValidator = new PublicSettingsValidator(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );
            Settings settings = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).build();
            publicSettingsValidator.validateSettings(settings);

            settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build();
            publicSettingsValidator.validateSettings(settings);

            settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build();
            publicSettingsValidator.validateSettings(settings);
        }
    }

    public void testNullSettings() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            // no operator privileges set on thread context
            PublicSettingsValidator publicSettingsValidator = new PublicSettingsValidator(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            publicSettingsValidator.validateSettings(null);
        }
    }

    public void testPublicSettingForNonOperator() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            // no operator privileges set on thread context

            PublicSettingsValidator publicSettingsValidator = new PublicSettingsValidator(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );
            Settings nonPublicSettings = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).build();
            var e = expectThrows(IllegalArgumentException.class, () -> publicSettingsValidator.validateSettings(nonPublicSettings));
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));

            Settings mixPublicNonPublic = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).put(PUBLIC_SETTING.getKey(), 0).build();
            e = expectThrows(IllegalArgumentException.class, () -> publicSettingsValidator.validateSettings(mixPublicNonPublic));
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));

            Settings publicSettings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build();
            // no exception
            publicSettingsValidator.validateSettings(publicSettings);
        }
    }
}
