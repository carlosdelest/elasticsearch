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

package co.elastic.elasticsearch.serverless.multiproject.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.EnumSet;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TransportGetProjectStatusActionTests extends ESTestCase {
    public void testCheckBlock() {
        var threadpool = mock(ThreadPool.class);
        var transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadpool);
        final var action = new TransportGetProjectStatusAction(
            transportService,
            mock(ClusterService.class),
            threadpool,
            mock(ActionFilters.class)
        );
        final ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(
                new ClusterBlock(
                    randomIntBetween(128, 256),
                    "metadata read block",
                    false,
                    false,
                    false,
                    RestStatus.SERVICE_UNAVAILABLE,
                    EnumSet.of(ClusterBlockLevel.METADATA_READ)
                )
            )
            .build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(8))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(
            new TransportGetProjectStatusAction.Request(TEST_REQUEST_TIMEOUT, randomUUID()),
            state
        );
        assertThat(e, not(nullValue()));
    }
}
