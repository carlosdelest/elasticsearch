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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.List;

public class GetDataStreamSettingsActionSettingsFilter extends ApiFilteringActionFilter<GetDataStreamSettingsAction.Response> {
    private final PublicSettingsFilter publicSettingsFilter;

    public GetDataStreamSettingsActionSettingsFilter(ThreadContext threadContext, IndexScopedSettings indexScopedSettings) {
        super(threadContext, GetDataStreamSettingsAction.NAME, GetDataStreamSettingsAction.Response.class);
        this.publicSettingsFilter = new PublicSettingsFilter(indexScopedSettings);
    }

    @Override
    protected GetDataStreamSettingsAction.Response filterResponse(GetDataStreamSettingsAction.Response response) throws Exception {
        List<GetDataStreamSettingsAction.DataStreamSettingsResponse> filteredDataStreamSettingsResponses = response
            .getDataStreamSettingsResponses()
            .stream()
            .map(
                r -> new GetDataStreamSettingsAction.DataStreamSettingsResponse(
                    r.dataStreamName(),
                    publicSettingsFilter.filter(r.settings()),
                    publicSettingsFilter.filter(r.effectiveSettings())
                )
            )
            .toList();
        return new GetDataStreamSettingsAction.Response(filteredDataStreamSettingsResponses);
    }
}
