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

package co.elastic.elasticsearch.api.filtering;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;

public class PublicSettingsFilterTests extends ESTestCase {

    public void testPublicSettingFilteringForNonOperator() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            // no operator privileges set on thread context

            PublicSettingsFilter publicSettingsValidator = new PublicSettingsFilter(MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS);
            Settings settings = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).build();
            assertThat(publicSettingsValidator.filter(settings), equalTo(Settings.EMPTY));

            settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build();
            assertThat(publicSettingsValidator.filter(settings), equalTo(settings));

            settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build();
            assertThat(publicSettingsValidator.filter(settings), equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build()));
        }
    }
}
