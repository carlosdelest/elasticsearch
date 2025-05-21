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

import org.elasticsearch.action.datastreams.GetDataStreamSettingsAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamSettingsActionSettingsFilterTests extends ESTestCase {
    GetDataStreamSettingsActionSettingsFilter filter = new GetDataStreamSettingsActionSettingsFilter(
        THREAD_CONTEXT,
        MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
    );

    public void testGetDataStreamSettingsResponseFiltering() throws Exception {
        try (ThreadContext.StoredContext ignored = THREAD_CONTEXT.stashContext()) {
            GetDataStreamSettingsAction.Response response = new GetDataStreamSettingsAction.Response(
                List.of(
                    new GetDataStreamSettingsAction.DataStreamSettingsResponse(
                        randomAlphanumericOfLength(20),
                        Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build(),
                        Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build()
                    ),
                    new GetDataStreamSettingsAction.DataStreamSettingsResponse(
                        randomAlphanumericOfLength(20),
                        Settings.builder().put(PUBLIC_SETTING.getKey(), 1).put(NON_PUBLIC_SETTING.getKey(), 1).build(),
                        Settings.builder().put(PUBLIC_SETTING.getKey(), 1).put(NON_PUBLIC_SETTING.getKey(), 1).build()
                    )
                )
            );
            GetDataStreamSettingsAction.Response newResponse = filter.filterResponse(response);

            assertThat(newResponse.getDataStreamSettingsResponses().size(), equalTo(response.getDataStreamSettingsResponses().size()));
            assertThat(
                newResponse.getDataStreamSettingsResponses().get(0).settings(),
                equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build())
            );
            assertThat(
                newResponse.getDataStreamSettingsResponses().get(0).effectiveSettings(),
                equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build())
            );
            assertThat(
                newResponse.getDataStreamSettingsResponses().get(1).settings(),
                equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 1).build())
            );
            assertThat(
                newResponse.getDataStreamSettingsResponses().get(1).effectiveSettings(),
                equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 1).build())
            );
        }
    }
}
