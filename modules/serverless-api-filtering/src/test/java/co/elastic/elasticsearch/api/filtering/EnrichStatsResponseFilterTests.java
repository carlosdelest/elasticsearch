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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EnrichStatsResponseFilterTests extends ESTestCase {

    public void testFilterEnrichStatsAction() {
        int numberOfNodes = randomIntBetween(0, 20);
        List<EnrichStatsAction.Response.ExecutingPolicy> executingPolicies = new ArrayList<>();
        List<EnrichStatsAction.Response.CoordinatorStats> coordinatorStats = new ArrayList<>();
        List<EnrichStatsAction.Response.CacheStats> cacheStats = new ArrayList<>();
        for (int i = 0; i < numberOfNodes; i++) {
            String nodeId = randomAlphaOfLength(20);
            coordinatorStats.add(
                new EnrichStatsAction.Response.CoordinatorStats(
                    nodeId,
                    randomIntBetween(0, 1000),
                    randomIntBetween(0, 1000),
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 100000)
                )
            );
            cacheStats.add(
                new EnrichStatsAction.Response.CacheStats(
                    nodeId,
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 10000),
                    randomIntBetween(0, 100000)
                )
            );
        }
        EnrichStatsAction.Response response = new EnrichStatsAction.Response(executingPolicies, coordinatorStats, cacheStats);
        EnrichStatsResponseFilter filter = new EnrichStatsResponseFilter(new ThreadContext(Settings.EMPTY));
        EnrichStatsAction.Response filteredResponse = filter.filterResponse(response);
        int expectedNumberOfNodes = numberOfNodes == 0 ? 0 : 1;
        assertThat(filteredResponse.getCoordinatorStats().size(), equalTo(expectedNumberOfNodes));
        assertThat(filteredResponse.getCacheStats().size(), equalTo(expectedNumberOfNodes));
        for (EnrichStatsAction.Response.CoordinatorStats filteredCoordinatorStats : filteredResponse.getCoordinatorStats()) {
            String nodeId = filteredCoordinatorStats.getNodeId();
            assertThat(nodeId, equalTo("serverless"));
        }
    }
}
