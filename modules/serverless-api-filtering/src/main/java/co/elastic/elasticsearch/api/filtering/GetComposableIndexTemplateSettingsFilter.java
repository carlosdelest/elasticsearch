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
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.Map;
import java.util.stream.Collectors;

public class GetComposableIndexTemplateSettingsFilter extends ApiFilteringActionFilter<GetComposableIndexTemplateAction.Response> {
    private final PublicSettingsFilter publicSettingsFilter;

    public GetComposableIndexTemplateSettingsFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, GetComponentTemplateAction.NAME, GetComposableIndexTemplateAction.Response.class);
        this.publicSettingsFilter = new PublicSettingsFilter(indexScopedSettings);
    }

    @Override
    protected GetComposableIndexTemplateAction.Response filterResponse(GetComposableIndexTemplateAction.Response response)
        throws Exception {
        Map<String, ComposableIndexTemplate> newTemplates = response.indexTemplates()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> mapComponentTemplate(e.getValue())));

        return new GetComposableIndexTemplateAction.Response(newTemplates, response.getGlobalRetention());
    }

    private ComposableIndexTemplate mapComponentTemplate(ComposableIndexTemplate t) {
        return ComposableIndexTemplate.builder().template(mapTemplate(t.template())).build();
    }

    private Template mapTemplate(Template template) {
        if (template.settings() != null) {
            Settings filter = publicSettingsFilter.filter(template.settings());
            return new Template(filter, template.mappings(), template.aliases(), template.lifecycle());
        }
        return template;
    }
}
