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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.action.MeteringIndexInfoService;

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
import java.util.concurrent.TimeUnit;
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

public class MeteringIndexInfoTaskExecutorTests extends ESTestCase {
    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;

    private Client client;
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private FeatureService featureService;

    private MeteringIndexInfoService meteringIndexInfoService;
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
        threadPool = new TestThreadPool(MeteringIndexInfoTaskExecutorTests.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        settings = Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true).build();
        clusterSettings = new ClusterSettings(
            settings,
            Stream.concat(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                Stream.of(MeteringIndexInfoTaskExecutor.ENABLED_SETTING, MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING)
            ).collect(Collectors.toSet())
        );
        clusterService = spy(createClusterService(threadPool, clusterSettings));
        localNodeId = clusterService.localNode().getId();
        persistentTasksService = mock(PersistentTasksService.class);
        meteringIndexInfoService = mock(MeteringIndexInfoService.class);
        featureService = new FeatureService(List.of(new MeteringFeatures()));
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
        var executor = MeteringIndexInfoTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            meteringIndexInfoService,
            settings
        );
        executor.startStopTask(new ClusterChangedEvent("", initialState(), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, times(1)).sendStartRequest(
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(new MeteringIndexInfoTaskParams()),
            any(),
            any()
        );
    }

    public void testSkippingTaskCreationIfItExists() {
        var executor = MeteringIndexInfoTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            meteringIndexInfoService,
            settings
        );
        executor.startStopTask(new ClusterChangedEvent("", stateWithLocalAssignedIndexSizeTask(initialState()), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, never()).sendStartRequest(
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(new MeteringIndexInfoTaskParams()),
            any(),
            any()
        );
    }

    public void testSkippingTaskCreationIfClusterDoesNotSupportFeature() {
        var executor = MeteringIndexInfoTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            meteringIndexInfoService,
            settings
        );
        executor.startStopTask(
            new ClusterChangedEvent("", stateWithUnassignedIndexSizeTask(initialStateWithoutFeature()), ClusterState.EMPTY_STATE)
        );
        verify(persistentTasksService, never()).sendStartRequest(
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(MeteringIndexInfoTask.TASK_NAME),
            eq(new MeteringIndexInfoTaskParams()),
            any(),
            any()
        );
    }

    public void testRunTaskOnNodeOperation() {
        var executor = MeteringIndexInfoTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            meteringIndexInfoService,
            settings
        );
        MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, new MeteringIndexInfoTaskParams(), state);
        verify(task, times(1)).run();
    }

    public void testAbortOnShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = MeteringIndexInfoTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                meteringIndexInfoService,
                settings
            );
            MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new MeteringIndexInfoTaskParams(), state);

            ClusterState initialState = stateWithLocalAssignedIndexSizeTask(initialState());
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState, type);
            executor.shuttingDown(new ClusterChangedEvent("shutdown node", withShutdown, initialState));
        }
        verify(persistentTasksService, times(REMOVE_SHUTDOWN_TYPES.size())).sendRemoveRequest(
            eq(MeteringIndexInfoTask.TASK_NAME),
            any(),
            any()
        );
    }

    public void testDoNothingIfAlreadyStoppedOnShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = MeteringIndexInfoTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                meteringIndexInfoService,
                settings
            );
            MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new MeteringIndexInfoTaskParams(), state);

            ClusterState initialState = initialState();
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState, type);
            executor.shuttingDown(new ClusterChangedEvent("shutdown node", withShutdown, initialState));
        }
        verify(persistentTasksService, never()).sendRemoveRequest(eq(MeteringIndexInfoTask.TASK_NAME), any(), any());
    }

    public void testDoNothingIfTaskAssignedToAnotherNodeOnShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = MeteringIndexInfoTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                meteringIndexInfoService,
                settings
            );
            MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new MeteringIndexInfoTaskParams(), state);

            ClusterState initialState = stateWithOtherAssignedIndexSizeTask(initialState());
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState, type);
            executor.shuttingDown(new ClusterChangedEvent("shutdown node", withShutdown, initialState));
        }
        verify(persistentTasksService, never()).sendRemoveRequest(eq(MeteringIndexInfoTask.TASK_NAME), any(), any());
    }

    public void testDoNothingIfAlreadyShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            var executor = MeteringIndexInfoTaskExecutor.create(
                client,
                clusterService,
                persistentTasksService,
                featureService,
                threadPool,
                meteringIndexInfoService,
                settings
            );
            MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new MeteringIndexInfoTaskParams(), state);

            ClusterState withShutdown = stateWithNodeShuttingDown(stateWithLocalAssignedIndexSizeTask(initialState()), type);
            executor.shuttingDown(new ClusterChangedEvent("unchanged", withShutdown, withShutdown));
            verify(persistentTasksService, never()).sendRemoveRequest(eq(MeteringIndexInfoTask.TASK_NAME), any(), any());
        }
    }

    public void testAbortOnDisable() {
        var executor = MeteringIndexInfoTaskExecutor.create(
            client,
            clusterService,
            persistentTasksService,
            featureService,
            threadPool,
            meteringIndexInfoService,
            settings
        );
        MeteringIndexInfoTask task = mock(MeteringIndexInfoTask.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, MeteringIndexInfoTaskParams.INSTANCE, state);

        clusterSettings.applySettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false).build());
        verify(persistentTasksService, never()).sendRemoveRequest(eq(MeteringIndexInfoTask.TASK_NAME), any(), any());
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
            .nodeFeatures(Map.of(localNodeId, Set.of(MeteringPlugin.INDEX_INFO_SUPPORTED.id())))
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
                    .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? tmpRandomTimeValue() : null)
                    .build()
            )
        );

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    @Deprecated(forRemoval = true) // replace with ESTestCase#randomTimeValue after https://github.com/elastic/elasticsearch/pull/107689
    private static TimeValue tmpRandomTimeValue() {
        return new TimeValue(between(0, 1000), randomFrom(TimeUnit.values()));
    }

    private ClusterState stateWithLocalAssignedIndexSizeTask(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(
            MeteringIndexInfoTask.TASK_NAME,
            MeteringIndexInfoTask.TASK_NAME,
            new MeteringIndexInfoTaskParams(),
            new PersistentTasksCustomMetadata.Assignment(localNodeId, "")
        );

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }

    private ClusterState stateWithOtherAssignedIndexSizeTask(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(
            MeteringIndexInfoTask.TASK_NAME,
            MeteringIndexInfoTask.TASK_NAME,
            new MeteringIndexInfoTaskParams(),
            new PersistentTasksCustomMetadata.Assignment("another-node", "")
        );

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }

    private ClusterState stateWithUnassignedIndexSizeTask(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(MeteringIndexInfoTask.TASK_NAME, MeteringIndexInfoTask.TASK_NAME, new MeteringIndexInfoTaskParams(), NO_NODE_FOUND);

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }
}
