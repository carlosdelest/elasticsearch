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
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction.Response.JobModelSnapshotUpgradeStats;

public class GetJobModelSnapshotsUpgradeStatsResponseFilter extends ApiFilteringActionFilter<
    GetJobModelSnapshotsUpgradeStatsAction.Response> {

    public GetJobModelSnapshotsUpgradeStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetJobModelSnapshotsUpgradeStatsAction.NAME, GetJobModelSnapshotsUpgradeStatsAction.Response.class);
    }

    /**
     * This method replaces the "node" sub-object from each set of stats in the response that has one
     * with the virtual "serverless" node.
     */
    @Override
    protected GetJobModelSnapshotsUpgradeStatsAction.Response filterResponse(GetJobModelSnapshotsUpgradeStatsAction.Response response) {
        QueryPage<JobModelSnapshotUpgradeStats> page = response.getResponse();
        if (page.count() == 0) {
            return response;
        } else {
            return new GetJobModelSnapshotsUpgradeStatsAction.Response(
                new QueryPage<>(
                    page.results().stream().map(GetJobModelSnapshotsUpgradeStatsResponseFilter::replaceNodeField).toList(),
                    page.count(),
                    page.getResultsField()
                )
            );
        }
    }

    static JobModelSnapshotUpgradeStats replaceNodeField(JobModelSnapshotUpgradeStats stats) {
        if (stats.getNode() == null) {
            return stats;
        } else {
            return new JobModelSnapshotUpgradeStats(
                stats.getJobId(),
                stats.getSnapshotId(),
                stats.getUpgradeState(),
                ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE,
                stats.getAssignmentExplanation()
            );
        }
    }
}
