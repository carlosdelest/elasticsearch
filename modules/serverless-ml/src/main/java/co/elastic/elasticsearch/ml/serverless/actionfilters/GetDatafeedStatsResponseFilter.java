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

package co.elastic.elasticsearch.ml.serverless.actionfilters;

import co.elastic.elasticsearch.ml.serverless.ServerlessMachineLearningExtension;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response.DatafeedStats;

public class GetDatafeedStatsResponseFilter extends ApiFilteringActionFilter<GetDatafeedsStatsAction.Response> {

    public GetDatafeedStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetDatafeedsStatsAction.NAME, GetDatafeedsStatsAction.Response.class);
    }

    /**
     * This method replaces the "node" sub-object from each set of stats in the response that has one
     * with the virtual "serverless" node.
     */
    @Override
    protected GetDatafeedsStatsAction.Response filterResponse(GetDatafeedsStatsAction.Response response) {
        QueryPage<DatafeedStats> page = response.getResponse();
        if (page.count() == 0) {
            return response;
        } else {
            return new GetDatafeedsStatsAction.Response(
                new QueryPage<>(
                    page.results().stream().map(GetDatafeedStatsResponseFilter::replaceNodeField).toList(),
                    page.count(),
                    page.getResultsField()
                )
            );
        }
    }

    static DatafeedStats replaceNodeField(DatafeedStats stats) {
        if (stats.getNode() == null) {
            return stats;
        } else {
            return new DatafeedStats(
                stats.getDatafeedId(),
                stats.getDatafeedState(),
                ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE,
                stats.getAssignmentExplanation(),
                stats.getTimingStats(),
                stats.getRunningState()
            );
        }
    }
}
