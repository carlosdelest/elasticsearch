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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.THREAD_CONTEXT;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;

@SuppressWarnings("unchecked")
public class PublicSettingsValidationActionFilterTests extends ESTestCase {

    Task task = Mockito.mock(Task.class);
    ActionRequest request = Mockito.mock(ActionRequest.class);
    ActionFilterChain<ActionRequest, ActionResponse> chain = Mockito.mock(ActionFilterChain.class);
    ActionListener<ActionResponse> listener = Mockito.mock(ActionListener.class);

    public void testCreateIndexValidation() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            PublicSettingsValidationActionFilter actionFilter = new PublicSettingsValidationActionFilter(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            CreateIndexRequest request = new CreateIndexRequest(
                "index",
                Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build()
            );

            // validation fails on public setting
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> actionFilter.apply(task, TransportCreateIndexAction.TYPE.name(), request, listener, chain)
            );
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
        }
    }

    public void testCreateComposedTemplateValidation() throws IOException {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            PublicSettingsValidationActionFilter actionFilter = new PublicSettingsValidationActionFilter(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            Settings mixPublicAndNonPublicSettings = Settings.builder()
                .put(PUBLIC_SETTING.getKey(), 0)
                .put(NON_PUBLIC_SETTING.getKey(), 0)
                .build();

            ComponentTemplate ct = new ComponentTemplate(new Template(mixPublicAndNonPublicSettings, null, null), 1L, Map.of());

            PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct);

            // validation fails on public setting
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> actionFilter.apply(task, PutComponentTemplateAction.NAME, request, listener, chain)
            );
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
        }
    }

    public void testCreateIndexComposableTemplateValidation() throws IOException {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            PublicSettingsValidationActionFilter actionFilter = new PublicSettingsValidationActionFilter(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            Settings mixPublicAndNonPublicSettings = Settings.builder()
                .put(PUBLIC_SETTING.getKey(), 0)
                .put(NON_PUBLIC_SETTING.getKey(), 0)
                .build();

            ComposableIndexTemplate ct = ComposableIndexTemplate.builder()
                .indexPatterns(List.of())
                .template(new Template(mixPublicAndNonPublicSettings, null, null))
                .componentTemplates(List.of())
                .priority(1L)
                .version(1L)
                .metadata(Map.of())
                .allowAutoCreate(false)
                .build();

            TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("my-ct")
                .indexTemplate(ct);

            // validation fails on public setting
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> actionFilter.apply(task, TransportPutComposableIndexTemplateAction.TYPE.name(), request, listener, chain)
            );
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
        }
    }

    public void testSettingsUpdateValidation() throws IOException {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            PublicSettingsValidationActionFilter actionFilter = new PublicSettingsValidationActionFilter(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            Settings mixPublicAndNonPublicSettings = Settings.builder()
                .put(PUBLIC_SETTING.getKey(), 0)
                .put(NON_PUBLIC_SETTING.getKey(), 0)
                .build();

            UpdateSettingsRequest request = new UpdateSettingsRequest(mixPublicAndNonPublicSettings);

            // validation fails on public setting
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> actionFilter.apply(task, TransportUpdateSettingsAction.TYPE.name(), request, listener, chain)
            );
            assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
        }
    }

    public void testNoValidationForNotDeclaredAction() {
        try (ThreadContext.StoredContext ctx = THREAD_CONTEXT.stashContext()) {
            PublicSettingsValidationActionFilter actionFilter = new PublicSettingsValidationActionFilter(
                THREAD_CONTEXT,
                MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS
            );

            actionFilter.apply(task, "someRandomName", request, listener, chain);

            Mockito.verify(chain).proceed(eq(task), eq("someRandomName"), eq(request), eq(listener));
        }
    }

}
