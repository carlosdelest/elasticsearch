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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.transforms.NodeAttributes;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetTransformStatsResponseFilterTests extends ESTestCase {

    public void testEmptyPage() {
        var filter = new GetTransformStatsResponseFilter(new ThreadContext(Settings.EMPTY));
        var response = new GetTransformStatsAction.Response(List.of());
        assertThat(filter.filterResponse(response), is(equalTo(response)));
    }

    public void testTransformStatsUnchangedWhenNodeInfoIsNull() {
        var filter = new GetTransformStatsResponseFilter(new ThreadContext(Settings.EMPTY));
        var response = new GetTransformStatsAction.Response(List.of(createTransformStats(null)));
        assertThat(filter.filterResponse(response), is(equalTo(response)));
    }

    public void testNodeInfoIsRemovedFromTransformStats() {
        var filter = new GetTransformStatsResponseFilter(new ThreadContext(Settings.EMPTY));
        var response = new GetTransformStatsAction.Response(
            List.of(
                createTransformStats(
                    new NodeAttributes(
                        "node-id",
                        "node-name",
                        "node-ephemeral-id",
                        "192.0.0.1",
                        Map.of("attr-1", "attr-1-value", "attr-2", "attr-2-value", "attr-3", "attr-3-value")
                    )
                )
            )
        );
        var expectedResponse = new GetTransformStatsAction.Response(
            List.of(createTransformStats(new NodeAttributes("serverless", "serverless", "serverless", "0.0.0.0", Map.of())))
        );
        assertThat(filter.filterResponse(response), is(equalTo(expectedResponse)));
    }

    private static TransformStats createTransformStats(NodeAttributes nodeAttributes) {
        return new TransformStats(
            "my-id",
            TransformStats.State.STARTED,
            "some-reason",
            nodeAttributes,
            new TransformIndexerStats(),
            TransformCheckpointingInfo.EMPTY,
            TransformHealth.GREEN
        );
    }
}
