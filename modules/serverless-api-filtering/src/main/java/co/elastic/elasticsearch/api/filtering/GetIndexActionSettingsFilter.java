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

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.Map;
import java.util.stream.Collectors;

public class GetIndexActionSettingsFilter extends ApiFilteringActionFilter<GetIndexResponse> {
    private final PublicSettingsFilter publicSettingsFilter;

    public GetIndexActionSettingsFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, GetIndexAction.NAME, GetIndexResponse.class);
        this.publicSettingsFilter = new PublicSettingsFilter(indexScopedSettings);
    }

    @Override
    protected GetIndexResponse filterResponse(GetIndexResponse r) {
        var settings = filter(r.getSettings());
        var defSettings = filter(r.getSettings());
        return new GetIndexResponse(r.indices(), r.mappings(), r.aliases(), settings, defSettings, r.dataStreams());
    }

    private Map<String, Settings> filter(Map<String, Settings> settings) {
        return settings.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> publicSettingsFilter.filter(e.getValue())));
    }
}
