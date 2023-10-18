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

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GetSettingsActionSettingsFilterTests extends ESTestCase {
    GetSettingsActionSettingsFilter filter = new GetSettingsActionSettingsFilter(
        THREAD_CONTEXT,
        MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
    );

    public void testGetIndicesActionResponseFiltering() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {

            GetSettingsResponse response = new GetSettingsResponse(
                Map.of("index", Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build()),
                Map.of("index", Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build())
            );

            GetSettingsResponse newResponse = filter.filterResponse(response);

            assertThat(
                newResponse.getIndexToDefaultSettings(),
                equalTo(Map.of("index", Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build()))
            );
            assertThat(
                newResponse.getIndexToDefaultSettings(),
                equalTo(Map.of("index", Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build()))
            );
        }
    }

    public void testGetIndicesActionResponseWithoutSettingsFiltering() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {

            GetSettingsResponse response = new GetSettingsResponse(null, null);

            GetSettingsResponse newResponse = filter.filterResponse(response);

            assertThat(newResponse.getIndexToDefaultSettings(), nullValue());
            assertThat(newResponse.getIndexToDefaultSettings(), nullValue());
        }

    }
}
