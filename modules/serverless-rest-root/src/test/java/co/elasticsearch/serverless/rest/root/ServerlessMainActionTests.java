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

package co.elasticsearch.serverless.rest.root;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.root.MainRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerlessMainActionTests extends ESTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(
                            randomIntBetween(1, 16),
                            "test global block 400",
                            randomBoolean(),
                            randomBoolean(),
                            false,
                            RestStatus.BAD_REQUEST,
                            ClusterBlockLevel.ALL
                        )
                    )
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(
                    new ClusterBlock(
                        randomIntBetween(1, 16),
                        "test global block 503",
                        randomBoolean(),
                        randomBoolean(),
                        false,
                        RestStatus.SERVICE_UNAVAILABLE,
                        ClusterBlockLevel.ALL
                    )
                )
                .build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        ServerlessTransportMainAction action = new ServerlessTransportMainAction(
            settings,
            transportService,
            mock(ActionFilters.class),
            clusterService
        );
        AtomicReference<ServerlessMainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ServerlessMainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        assertThat(responseRef.get().getClusterName().value(), equalTo("elasticsearch"));
        assertThat(responseRef.get().getClusterUuid(), equalTo(state.metadata().clusterUUID()));
        assertThat(responseRef.get().getNodeName(), equalTo("my-node"));
        verify(clusterService, times(1)).state();
    }
}
