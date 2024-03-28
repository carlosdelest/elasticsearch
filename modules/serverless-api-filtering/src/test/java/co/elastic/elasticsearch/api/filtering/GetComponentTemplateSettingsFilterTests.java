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

import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomGlobalRetention;
import static org.hamcrest.Matchers.equalTo;

public class GetComponentTemplateSettingsFilterTests extends ESTestCase {

    GetComponentTemplateSettingsFilter filter = new GetComponentTemplateSettingsFilter(
        THREAD_CONTEXT,
        MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
    );

    public void testGetComponentTemplateSettingsWithMixedSettings() throws Exception {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            Settings mixPublicAndNonPublicSettings = Settings.builder()
                .put(PUBLIC_SETTING.getKey(), 0)
                .put(NON_PUBLIC_SETTING.getKey(), 0)
                .build();

            ComponentTemplate ct = new ComponentTemplate(new Template(mixPublicAndNonPublicSettings, null, null), 1L, Map.of());

            GetComponentTemplateAction.Response response = new GetComponentTemplateAction.Response(
                Map.of("name", ct),
                randomGlobalRetention()
            );

            GetComponentTemplateAction.Response newResponse = filter.filterResponse(response);

            assertThat(
                newResponse.getComponentTemplates().get("name").template().settings(),
                equalTo(Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build())
            );
            assertThat(newResponse.getGlobalRetention(), equalTo(response.getGlobalRetention()));
        }
    }

    public void testGetComponentTemplateWithoutSettings() throws Exception {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            ComponentTemplate ct = new ComponentTemplate(new Template(null, null, null), 1L, Map.of());

            GetComponentTemplateAction.Response response = new GetComponentTemplateAction.Response(
                Map.of("name", ct),
                randomGlobalRetention()
            );

            GetComponentTemplateAction.Response newResponse = filter.filterResponse(response);

            assertThat(newResponse.getComponentTemplates().get("name").template().settings(), equalTo(null));
            assertThat(newResponse.getGlobalRetention(), equalTo(response.getGlobalRetention()));
        }
    }
}
