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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.Map;
import java.util.stream.Collectors;

public class GetComponentTemplateSettingsFilter extends ApiFilteringActionFilter<GetComponentTemplateAction.Response> {
    private final PublicSettingsFilter publicSettingsFilter;

    protected GetComponentTemplateSettingsFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, GetComponentTemplateAction.NAME, GetComponentTemplateAction.Response.class);
        this.publicSettingsFilter = new PublicSettingsFilter(indexScopedSettings);
    }

    @Override
    protected GetComponentTemplateAction.Response filterResponse(GetComponentTemplateAction.Response response) throws Exception {
        Map<String, ComponentTemplate> newComponentTemplates = response.getComponentTemplates()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> mapComponentTemplate(e.getValue())));

        return new GetComponentTemplateAction.Response(
            newComponentTemplates,
            response.getRolloverConfiguration(),
            response.getGlobalRetention()
        );
    }

    private ComponentTemplate mapComponentTemplate(ComponentTemplate componentTemplate) {
        Template template = componentTemplate.template();
        Map<String, Object> metadata = componentTemplate.metadata();
        Long version = componentTemplate.version();
        return new ComponentTemplate(mapTemplate(template), version, metadata);
    }

    private Template mapTemplate(Template template) {
        if (template.settings() != null) {
            Settings filter = publicSettingsFilter.filter(template.settings());
            return new Template(filter, template.mappings(), template.aliases(), template.lifecycle());
        }
        return template;
    }
}
