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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.NON_PUBLIC_SETTING;
import static co.elastic.elasticsearch.api.filtering.CommonTestPublicSettings.PUBLIC_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;

public class PublicSettingsValidatorTests extends ESTestCase {

    ThreadContext threadContext;
    PublicSettingsValidator<?> genericValidator;
    Task task = Mockito.mock(Task.class);
    ActionRequest request = new DummyRequest();
    @SuppressWarnings("unchecked")
    ActionFilterChain<ActionRequest, ActionResponse> chain = Mockito.mock(ActionFilterChain.class);
    @SuppressWarnings("unchecked")
    ActionListener<ActionResponse> listener = Mockito.mock(ActionListener.class);

    static class DummyRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    @Before
    public void setupValidator() {
        threadContext = new ThreadContext(Settings.EMPTY);
        genericValidator = new PublicSettingsValidator<DummyRequest>(threadContext, MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS) {
            @Override
            protected Settings getSettingsFromRequest(DummyRequest request) {
                return Settings.EMPTY;
            }

            @Override
            public String actionName() {
                return "testaction";
            }
        };
    }

    public void testPublicSettingAllowedForOperator() {
        // set operator privileges
        threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);

        Settings settings = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).build();
        genericValidator.validateSettings(settings);

        settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build();
        genericValidator.validateSettings(settings);

        settings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build();
        genericValidator.validateSettings(settings);
    }

    // in some contexts i.e. index templates, settings in a request might be missing a prefix (index.)
    // this is fixed in a transport action by SettingsBuilder#normalizePrefix
    public void testValidationOfSettingsThatAreNormalised() {
        var internalSettings = Settings.builder().put("internal_setting", 0).build();
        var e = expectThrows(IllegalArgumentException.class, () -> genericValidator.validateSettings(internalSettings));
        assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));

        var publicSettings = Settings.builder().put("public_setting", 0).build();
        genericValidator.validateSettings(publicSettings);

        var mixedSettings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build();
        e = expectThrows(IllegalArgumentException.class, () -> genericValidator.validateSettings(mixedSettings));
        assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
    }

    public void testDoNotFailOnUnknownSetting() {
        var settings = Settings.builder().put("unknownSettingName", 0).build();
        genericValidator.validateSettings(settings);
    }

    public void testValidationOfAffixSetting() {
        var internalSettings = Settings.builder().put("index.private.affix.settingName", 0).build();
        var e = expectThrows(IllegalArgumentException.class, () -> genericValidator.validateSettings(internalSettings));
        assertThat(e.getMessage(), equalTo("Settings [index.private.affix.settingName] are not available when running in serverless mode"));

        var publicSettings = Settings.builder().put("index.public.affix.settingName", 0).build();
        genericValidator.validateSettings(publicSettings);
    }

    public void testNullSettings() {
        // no operator privileges set on thread context
        genericValidator.validateSettings(null);
    }

    public void testPublicSettingForNonOperator() {
        // no operator privileges set on thread context

        Settings nonPublicSettings = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).build();
        var e = expectThrows(IllegalArgumentException.class, () -> genericValidator.validateSettings(nonPublicSettings));
        assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));

        Settings mixPublicNonPublic = Settings.builder().put(NON_PUBLIC_SETTING.getKey(), 0).put(PUBLIC_SETTING.getKey(), 0).build();
        e = expectThrows(IllegalArgumentException.class, () -> genericValidator.validateSettings(mixPublicNonPublic));
        assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));

        Settings publicSettings = Settings.builder().put(PUBLIC_SETTING.getKey(), 0).build();
        // no exception
        genericValidator.validateSettings(publicSettings);
    }

    public void testCreateIndexValidation() {
        CreateIndexRequest request = new CreateIndexRequest(
            "index",
            Settings.builder().put(PUBLIC_SETTING.getKey(), 0).put(NON_PUBLIC_SETTING.getKey(), 0).build()
        );

        assertValidatorFails(() -> new CreateIndexSettingsValidator(threadContext, MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS), request);
    }

    public void testCreateComposedTemplateValidation() throws IOException {
        Settings mixPublicAndNonPublicSettings = Settings.builder()
            .put(PUBLIC_SETTING.getKey(), 0)
            .put(NON_PUBLIC_SETTING.getKey(), 0)
            .build();

        ComponentTemplate ct = new ComponentTemplate(new Template(mixPublicAndNonPublicSettings, null, null), 1L, Map.of());

        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request("my-ct").componentTemplate(ct);

        assertValidatorFails(
            () -> new PutComponentTemplateSettingsValidator(threadContext, MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS),
            request
        );
    }

    public void testCreateIndexComposableTemplateValidation() throws IOException {
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

        assertValidatorFails(
            () -> new PutComposableTemplateSettingsValidator(threadContext, MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS),
            request
        );
    }

    public void testSettingsUpdateValidation() throws IOException {
        Settings mixPublicAndNonPublicSettings = Settings.builder()
            .put(PUBLIC_SETTING.getKey(), 0)
            .put(NON_PUBLIC_SETTING.getKey(), 0)
            .build();

        UpdateSettingsRequest request = new UpdateSettingsRequest(mixPublicAndNonPublicSettings);
        assertValidatorFails(() -> new UpdateSettingsValidator(threadContext, MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS), request);
    }

    <RequestType extends ActionRequest, Validator extends PublicSettingsValidator<RequestType>> void assertValidatorFails(
        Supplier<Validator> ctor,
        RequestType request
    ) {
        Validator v = ctor.get();
        // validation fails on public setting
        var e = expectThrows(IllegalArgumentException.class, () -> v.apply(task, v.actionName(), request, listener, chain));
        assertThat(e.getMessage(), equalTo("Settings [index.internal_setting] are not available when running in serverless mode"));
    }

    public void testNoValidationForNotDeclaredAction() {
        genericValidator.apply(task, "someRandomName", request, listener, chain);

        Mockito.verify(chain).proceed(eq(task), eq("someRandomName"), eq(request), eq(listener));
    }
}
