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

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.Map;
import java.util.stream.Collectors;

public class GetSettingsActionSettingsFilter extends ApiFilteringActionFilter<GetSettingsResponse> {
    private final PublicSettingsFilter publicSettingsFilter;

    public GetSettingsActionSettingsFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, GetSettingsAction.NAME, GetSettingsResponse.class);
        this.publicSettingsFilter = new PublicSettingsFilter(indexScopedSettings);
    }

    @Override
    protected GetSettingsResponse filterResponse(GetSettingsResponse r) {
        Map<String, Settings> indexToSettings = filter(r.getIndexToSettings());
        Map<String, Settings> indexToDefaultSettings = filter(r.getIndexToDefaultSettings());

        return new GetSettingsResponse(indexToSettings, indexToDefaultSettings);
    }

    private Map<String, Settings> filter(Map<String, Settings> settings) {
        if (settings != null) {
            return settings.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> publicSettingsFilter.filter(e.getValue())));
        }
        return settings;
    }
}
