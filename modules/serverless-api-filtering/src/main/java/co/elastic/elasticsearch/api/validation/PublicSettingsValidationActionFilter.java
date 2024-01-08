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
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;

import java.util.Map;
import java.util.function.Function;

/**
 * An action filter that performs a validation of incoming requests with settings object.
 * The validation is preventing public users to use non-public settings.
 */
public class PublicSettingsValidationActionFilter implements ActionFilter {
    private final PublicSettingsValidator publicSettingsValidator;

    private final Map<String, Function<? extends ActionRequest, Settings>> mappingFunctions = Map.ofEntries(
        settingsFromUpdateSettingsAction(),
        settingsFromCreateIndexAction(),
        settingsFromCreateComponentTemplate(),
        settingsFromCreateIndexComposableTemplate()
    );

    private Map.Entry<String, Function<UpdateSettingsRequest, Settings>> settingsFromUpdateSettingsAction() {
        return Map.entry(TransportUpdateSettingsAction.TYPE.name(), (UpdateSettingsRequest ir) -> ir.settings());
    }

    private Map.Entry<String, Function<CreateIndexRequest, Settings>> settingsFromCreateIndexAction() {
        return Map.entry(CreateIndexAction.NAME, (CreateIndexRequest ir) -> ir.settings());
    }

    private Map.Entry<String, Function<PutComponentTemplateAction.Request, Settings>> settingsFromCreateComponentTemplate() {
        return Map.entry(PutComponentTemplateAction.NAME, (PutComponentTemplateAction.Request r) -> {
            if (r.componentTemplate() != null) {
                if (r.componentTemplate().template() != null) {
                    return r.componentTemplate().template().settings();
                }
            }
            return null;
        });
    }

    private
        Map.Entry<String, Function<TransportPutComposableIndexTemplateAction.Request, Settings>>
        settingsFromCreateIndexComposableTemplate() {
        return Map.entry(TransportPutComposableIndexTemplateAction.TYPE.name(), (TransportPutComposableIndexTemplateAction.Request r) -> {
            if (r.indexTemplate() != null) {
                if (r.indexTemplate().template() != null) {
                    return r.indexTemplate().template().settings();
                }
            }
            return null;
        });
    }

    public PublicSettingsValidationActionFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        this.publicSettingsValidator = new PublicSettingsValidator(threadContext, indexScopedSettings);
    }

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
        if (mappingFunctions.containsKey(action)) {
            // casting??
            Function<Request, Settings> settingsFunction = (Function<Request, Settings>) mappingFunctions.get(action);
            Settings apply = settingsFunction.apply(request);
            publicSettingsValidator.validateSettings(apply);
        }

        chain.proceed(task, action, request, listener);
    }

}
