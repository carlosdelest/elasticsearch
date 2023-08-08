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
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;

public class GetJobStatsResponseFilter extends ApiFilteringActionFilter<GetJobsStatsAction.Response> {

    public GetJobStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetJobsStatsAction.NAME, GetJobsStatsAction.Response.class);
    }

    /**
     * This method replaces the "node" sub-object from each set of stats in the response that has one
     * with the virtual "serverless" node.
     */
    @Override
    protected GetJobsStatsAction.Response filterResponse(GetJobsStatsAction.Response response) {
        QueryPage<JobStats> page = response.getResponse();
        if (page.count() == 0) {
            return response;
        } else {
            return new GetJobsStatsAction.Response(
                new QueryPage<>(
                    page.results().stream().map(GetJobStatsResponseFilter::replaceNodeField).toList(),
                    page.count(),
                    page.getResultsField()
                )
            );
        }
    }

    static JobStats replaceNodeField(JobStats stats) {
        if (stats.getNode() == null) {
            return stats;
        } else {
            return new JobStats(
                stats.getJobId(),
                stats.getDataCounts(),
                stats.getModelSizeStats(),
                stats.getForecastStats(),
                stats.getState(),
                ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE,
                stats.getAssignmentExplanation(),
                stats.getOpenTime(),
                stats.getTimingStats()
            );
        }
    }
}
