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

import co.elastic.elasticsearch.api.validation.CreateIndexSettingsValidator;
import co.elastic.elasticsearch.api.validation.IndicesAliasRequestValidator;
import co.elastic.elasticsearch.api.validation.PutComponentTemplateDataStreamLifecycleValidator;
import co.elastic.elasticsearch.api.validation.PutComponentTemplateSettingsValidator;
import co.elastic.elasticsearch.api.validation.PutComposableIndexTemplateDataStreamLifecycleValidator;
import co.elastic.elasticsearch.api.validation.PutComposableTemplateSettingsValidator;
import co.elastic.elasticsearch.api.validation.PutDataStreamLifecycleValidator;
import co.elastic.elasticsearch.api.validation.ReindexRequestValidator;
import co.elastic.elasticsearch.api.validation.RolloverRequestValidator;
import co.elastic.elasticsearch.api.validation.SimulateIndexTemplateDataStreamLifecycleValidator;
import co.elastic.elasticsearch.api.validation.SimulateTemplateDataStreamLifecycleValidator;
import co.elastic.elasticsearch.api.validation.UpdateSettingsValidator;

import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/*
 * This plugin is meant to be a catch-all for API filters that don't neatly fit into an existing serverless module.
 */
public class ServerlessApiFilteringPlugin extends Plugin implements ActionPlugin {

    private final AtomicReference<List<ActionFilter>> actionFilters = new AtomicReference<>();

    public ServerlessApiFilteringPlugin() {}

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ThreadContext context = services.threadPool().getThreadContext();
        IndexScopedSettings indexScopedSettings = services.indicesService().getIndexScopedSettings();

        actionFilters.set(
            List.of(
                new TaskResponseFilter(context),
                new GetComponentTemplateSettingsFilter(context, indexScopedSettings),
                new GetIndexActionSettingsFilter(context, indexScopedSettings),
                new GetSettingsActionSettingsFilter(context, indexScopedSettings),
                new UpdateSettingsValidator(context, indexScopedSettings),
                new CreateIndexSettingsValidator(context, indexScopedSettings),
                new PutComponentTemplateSettingsValidator(context, indexScopedSettings),
                new PutComposableTemplateSettingsValidator(context, indexScopedSettings),
                new ReindexRequestValidator(),
                new RolloverRequestValidator(context),
                new PutComponentTemplateDataStreamLifecycleValidator(context),
                new PutComposableIndexTemplateDataStreamLifecycleValidator(context),
                new PutDataStreamLifecycleValidator(context),
                new SimulateIndexTemplateDataStreamLifecycleValidator(context),
                new SimulateTemplateDataStreamLifecycleValidator(context),
                new IndicesAliasRequestValidator()
            )
        );

        return List.of();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return actionFilters.get();
    }
}
