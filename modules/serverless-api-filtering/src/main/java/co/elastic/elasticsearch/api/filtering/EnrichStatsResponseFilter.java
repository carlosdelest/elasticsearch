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

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CacheStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;

import java.util.List;

/**
 * This ApiFilteringActionFilter redacts node information from an EnrichStatsAction response, rolling up all coordinator stats and cache
 * stats into a single pseudo node named "serverless".
 */
public class EnrichStatsResponseFilter extends ApiFilteringActionFilter<EnrichStatsAction.Response> {

    public EnrichStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, EnrichStatsAction.NAME, EnrichStatsAction.Response.class);
    }

    /*
     * This method rolls up the coordinator stats and cache stats for all nodes in the response into stats for a single pseudo node named
     * "serverless". The stats are rolled up by adding all of the values for all nodes into a single stat object.
     */
    @Override
    protected EnrichStatsAction.Response filterResponse(EnrichStatsAction.Response response) {
        return new EnrichStatsAction.Response(
            response.getExecutingPolicies(),
            rollupCoordinatorStats(response.getCoordinatorStats()),
            rollupCacheStats(response.getCacheStats())
        );
    }

    private static List<CoordinatorStats> rollupCoordinatorStats(List<CoordinatorStats> coordinatorStatsList) {
        if (coordinatorStatsList.isEmpty()) {
            return coordinatorStatsList;
        }
        int totalQueueSize = 0;
        int totalRemoteRequestsCurrent = 0;
        long totalRemoteRequestsTotal = 0;
        long totalExecutedSearchesTotal = 0;
        for (CoordinatorStats stats : coordinatorStatsList) {
            totalQueueSize += stats.getQueueSize();
            totalRemoteRequestsCurrent += stats.getRemoteRequestsCurrent();
            totalRemoteRequestsTotal += stats.getRemoteRequestsTotal();
            totalExecutedSearchesTotal += stats.getExecutedSearchesTotal();
        }
        return List.of(
            new CoordinatorStats(
                "serverless",
                totalQueueSize,
                totalRemoteRequestsCurrent,
                totalRemoteRequestsTotal,
                totalExecutedSearchesTotal
            )
        );
    }

    private static List<CacheStats> rollupCacheStats(List<CacheStats> cacheStatsList) {
        if (cacheStatsList.isEmpty()) {
            return cacheStatsList;
        }
        long totalCount = 0;
        long totalHits = 0;
        long totalMisses = 0;
        long totalEvictions = 0;
        for (CacheStats stats : cacheStatsList) {
            totalCount += stats.getCount();
            totalHits += stats.getHits();
            totalMisses += stats.getMisses();
            totalEvictions += stats.getEvictions();
        }
        return List.of(new CacheStats("serverless", totalCount, totalHits, totalMisses, totalEvictions));
    }
}
