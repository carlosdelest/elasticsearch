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

package co.elastic.elasticsearch.serverless.indexsize;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class IndexSizeTaskExecutorTests extends ESTestCase {
    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;

    private Client client;
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private FeatureService featureService;
    private String localNodeId;
    private ClusterSettings clusterSettings;
    private Settings settings;

    private static final List<SingleNodeShutdownMetadata.Type> REMOVE_SHUTDOWN_TYPES = List.of(
        SingleNodeShutdownMetadata.Type.RESTART,
        SingleNodeShutdownMetadata.Type.REMOVE,
        SingleNodeShutdownMetadata.Type.SIGTERM
    );

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(IndexSizeTaskExecutorTests.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        settings = Settings.builder().put(IndexSizeTaskExecutor.ENABLED_SETTING.getKey(), true).build();
        clusterSettings = new ClusterSettings(
            settings,
            Stream.concat(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                Stream.of(IndexSizeTaskExecutor.ENABLED_SETTING, IndexSizeTaskExecutor.POLL_INTERVAL_SETTING)
            ).collect(Collectors.toSet())
        );
        clusterService = spy(createClusterService(threadPool, clusterSettings));
        localNodeId = clusterService.localNode().getId();
        persistentTasksService = mock(PersistentTasksService.class);
        featureService = new FeatureService(List.of(new IndexSizeFeatures()));
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    public void testTaskCreation() {
        var executor = IndexSizeTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            null,
            settings
        );
        executor.startStopTask(new ClusterChangedEvent("", initialState(), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, times(1)).sendStartRequest(
            eq(IndexSizeTask.TASK_NAME),
            eq(IndexSizeTask.TASK_NAME),
            eq(new IndexSizeTaskParams()),
            any()
        );
    }

    public void testSkippingTaskCreationIfItExists() {
        var executor = IndexSizeTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            null,
            settings
        );
        executor.startStopTask(new ClusterChangedEvent("", stateWithIndexSizeTask(initialState()), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, never()).sendStartRequest(
            eq(IndexSizeTask.TASK_NAME),
            eq(IndexSizeTask.TASK_NAME),
            eq(new IndexSizeTaskParams()),
            any()
        );
    }

    public void testSkippingTaskCreationIfClusterDoesNotSupportFeature() {
        var executor = IndexSizeTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            null,
            settings
        );
        executor.startStopTask(new ClusterChangedEvent("", stateWithIndexSizeTask(initialStateWithoutFeature()), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, never()).sendStartRequest(
            eq(IndexSizeTask.TASK_NAME),
            eq(IndexSizeTask.TASK_NAME),
            eq(new IndexSizeTaskParams()),
            any()
        );
    }

    public void testRunTaskOnNodeOperation() {
        var executor = IndexSizeTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            null,
            settings
        );
        IndexSizeTask task = mock(IndexSizeTask.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, new IndexSizeTaskParams(), state);
        verify(task, times(1)).run();
    }

    public void testAbortOnShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = IndexSizeTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                null,
                settings
            );
            IndexSizeTask task = mock(IndexSizeTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new IndexSizeTaskParams(), state);

            ClusterState initialState = stateWithIndexSizeTask(initialState());
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState, type);
            executor.shuttingDown(new ClusterChangedEvent("shutdown node", withShutdown, initialState));
        }
        verify(persistentTasksService, times(REMOVE_SHUTDOWN_TYPES.size())).sendRemoveRequest(eq(IndexSizeTask.TASK_NAME), any());
    }

    public void testDoNothingIfAlreadyShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = IndexSizeTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                null,
                settings
            );
            IndexSizeTask task = mock(IndexSizeTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new IndexSizeTaskParams(), state);

            ClusterState withShutdown = stateWithNodeShuttingDown(stateWithIndexSizeTask(initialState()), type);
            executor.shuttingDown(new ClusterChangedEvent("unchanged", withShutdown, withShutdown));
            verify(persistentTasksService, never()).sendRemoveRequest(eq(IndexSizeTask.TASK_NAME), any());
        }
    }

    public void testAbortOnDisable() {
        var executor = IndexSizeTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            null,
            settings
        );
        IndexSizeTask task = mock(IndexSizeTask.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, IndexSizeTaskParams.INSTANCE, state);

        clusterSettings.applySettings(Settings.builder().put(IndexSizeTaskExecutor.ENABLED_SETTING.getKey(), false).build());
        verify(persistentTasksService, never()).sendRemoveRequest(eq(IndexSizeTask.TASK_NAME), any());
    }

    private ClusterState initialStateWithoutFeature() {
        Metadata.Builder metadata = Metadata.builder();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNodeUtils.create(localNodeId));
        nodes.localNodeId(localNodeId);
        nodes.masterNodeId(localNodeId);

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).build();
    }

    private ClusterState initialState() {
        return ClusterState.builder(initialStateWithoutFeature())
            .nodeFeatures(Map.of(localNodeId, Set.of(IndexSizePlugin.INDEX_SIZE_SUPPORTED.id())))
            .build();
    }

    private ClusterState stateWithNodeShuttingDown(ClusterState clusterState, SingleNodeShutdownMetadata.Type type) {
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(
            Collections.singletonMap(
                localNodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(localNodeId)
                    .setReason("shutdown for a unit test")
                    .setType(type)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setGracePeriod(
                        type == SingleNodeShutdownMetadata.Type.SIGTERM
                            ? TimeValue.parseTimeValue(randomTimeValue(), this.getTestName())
                            : null
                    )
                    .build()
            )
        );

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    private ClusterState stateWithIndexSizeTask(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(IndexSizeTask.TASK_NAME, IndexSizeTask.TASK_NAME, new IndexSizeTaskParams(), NO_NODE_FOUND);

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }
}
