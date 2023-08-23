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

package co.elastic.elasticsearch.serverless.transform;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.transforms.NodeAttributes;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;

import java.util.List;
import java.util.Map;

public class GetTransformStatsResponseFilter extends ApiFilteringActionFilter<GetTransformStatsAction.Response> {

    private static final NodeAttributes SERVERLESS_VIRTUAL_TRANSFORM_NODE_ATTRIBUTES = new NodeAttributes(
        "serverless",
        "serverless",
        "serverless",
        NetworkAddress.format(TransportAddress.META_ADDRESS),
        Map.of()
    );

    public GetTransformStatsResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetTransformStatsAction.NAME, GetTransformStatsAction.Response.class);
    }

    /**
     * This method replaces the "node" sub-object from each set of stats in the response that has one
     * with the virtual "serverless" node.
     */
    @Override
    protected GetTransformStatsAction.Response filterResponse(GetTransformStatsAction.Response response) {
        List<TransformStats> page = response.getTransformsStats();
        if (page.isEmpty()) {
            return response;
        } else {
            return new GetTransformStatsAction.Response(
                new QueryPage<>(
                    page.stream().map(GetTransformStatsResponseFilter::replaceNodeField).toList(),
                    page.size(),
                    response.getResultsField()
                )
            );
        }
    }

    static TransformStats replaceNodeField(TransformStats stats) {
        if (stats.getNode() == null) {
            return stats;
        } else {
            return new TransformStats(
                stats.getId(),
                stats.getState(),
                stats.getReason(),
                SERVERLESS_VIRTUAL_TRANSFORM_NODE_ATTRIBUTES,
                stats.getIndexerStats(),
                stats.getCheckpointingInfo(),
                stats.getHealth()
            );
        }
    }
}
