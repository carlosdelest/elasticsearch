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

import co.elastic.elasticsearch.metering.action.GetMeteringStatsAction;

import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, minNumDataNodes = 3)
public class MeteringTransportActionsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class, MeteringPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), false)
            .build();
    }

    @After
    public void cleanUp() {
        updateClusterSettings(
            Settings.builder()
                .putNull(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey())
        );
    }

    public void testActionRetriedUntilTaskNodeAssigned() throws Exception {
        var getMeteringStatsHandled = new AtomicInteger();

        updateClusterSettings(
            Settings.builder().put(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
        );

        var masterNode = clusterService().state().nodes().getMasterNode();
        var transportService = MockTransportService.getInstance(masterNode.getName());

        transportService.addRequestHandlingBehavior(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME, (handler, request, channel, task) -> {
            getMeteringStatsHandled.incrementAndGet();
            handler.messageReceived(request, channel, task);
        });

        PlainActionFuture<GetMeteringStatsAction.Response> listener = new PlainActionFuture<>();
        // Start a request before enabling the task executor
        transportService.sendRequest(
            masterNode,
            GetMeteringStatsAction.FOR_SECONDARY_USER_NAME,
            new GetMeteringStatsAction.Request(),
            new ActionListenerResponseHandler<>(listener, GetMeteringStatsAction.Response::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );

        assertFalse(listener.isDone());

        // Enable the executor, and wait for a node to be assigned
        updateClusterSettings(Settings.builder().put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true));
        var persistentTaskNode = getMeteringPersistentTaskAssignedNode();

        // Now we have an assigned node, and the request is completed.
        assertThat(persistentTaskNode, notNullValue());
        var result = listener.get(5, TimeUnit.SECONDS);
        assertThat(result, notNullValue());
        assertThat(getMeteringStatsHandled.get(), greaterThanOrEqualTo(1));
    }

    public void testActionRetriedWhenTaskNodeSwitched() throws Exception {
        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );

        updateClusterSettings(
            Settings.builder()
                .put(MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                .put(MeteringIndexInfoTaskExecutor.ENABLED_SETTING.getKey(), true)
        );

        var persistentTaskNode1 = getMeteringPersistentTaskAssignedNode();

        assertThat(persistentTaskNode1, notNullValue());

        // Now we have an assigned node, intercept GetMeteringStatsAction requests there
        var requestOnOldTaskNodeLatch = new CountDownLatch(1);
        var newTaskNodeReadyLatch = new CountDownLatch(1);
        var persistentTaskNode1Requests = new AtomicInteger();
        var persistentTaskNode1TransportService = MockTransportService.getInstance(persistentTaskNode1.getName());
        persistentTaskNode1TransportService.addRequestHandlingBehavior(
            GetMeteringStatsAction.FOR_SECONDARY_USER_NAME,
            (handler, request, channel, task) -> {
                logger.info("GetMeteringStatsAction Request received on OLD PersistentTaskNode[{}]", persistentTaskNode1.getName());
                persistentTaskNode1Requests.incrementAndGet();
                requestOnOldTaskNodeLatch.countDown();
                safeAwait(newTaskNodeReadyLatch);
                logger.info(
                    "GetMeteringStatsAction start handling messageReceived on OLD PersistentTaskNode[{}]",
                    persistentTaskNode1.getName()
                );
                handler.messageReceived(request, channel, task);
                logger.info(
                    "GetMeteringStatsAction finished handling messageReceived on OLD PersistentTaskNode[{}]",
                    persistentTaskNode1.getName()
                );
            }
        );

        // Send the request to another node, wait for it to be forwarded to persistentTaskNode1
        PlainActionFuture<GetMeteringStatsAction.Response> listener = new PlainActionFuture<>();
        var notPersistentTaskNode = clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.equals(persistentTaskNode1) == false)
            .findFirst();
        assertThat(notPersistentTaskNode, isPresent());
        assertThat(notPersistentTaskNode.get().getName(), not(equalTo(persistentTaskNode1.getName())));
        var transportService = MockTransportService.getInstance(notPersistentTaskNode.get().getName());
        transportService.sendRequest(
            notPersistentTaskNode.get(),
            GetMeteringStatsAction.FOR_SECONDARY_USER_NAME,
            new GetMeteringStatsAction.Request(),
            new ActionListenerResponseHandler<>(
                listener,
                GetMeteringStatsAction.Response::new,
                transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
            )
        );
        safeAwait(requestOnOldTaskNodeLatch);
        persistentTaskNode1TransportService.clearAllRules();

        // Now forcing the task to be re-assigned (with a chance to switch to another node)
        PlainActionFuture<PersistentTasksCustomMetadata.PersistentTask<?>> unassignmentFuture = new PlainActionFuture<>();

        var currentTask = MeteringIndexInfoTask.findTask(clusterService().state());

        persistentTasksClusterService.unassignPersistentTask(
            currentTask.getId(),
            currentTask.getAllocationId(),
            "unassignment test",
            unassignmentFuture
        );
        PersistentTasksCustomMetadata.PersistentTask<?> unassignedTask = unassignmentFuture.get();
        assertThat(unassignedTask.getId(), equalTo(currentTask.getId()));
        assertThat(unassignedTask.getAssignment().getExecutorNode(), is(nullValue()));

        // Verifying the task runs again
        var persistentTaskNode2 = getMeteringPersistentTaskAssignedNode();

        assertThat(persistentTaskNode2, notNullValue());
        logger.info("PersistentTask node is now [{}]", persistentTaskNode2.getName());

        // Intercept GetMeteringStatsAction on the new node too
        var persistentTaskNode2Requests = new AtomicInteger();
        MockTransportService.getInstance(persistentTaskNode2.getName())
            .addRequestHandlingBehavior(GetMeteringStatsAction.FOR_SECONDARY_USER_NAME, (handler, request, channel, task) -> {
                logger.info("GetMeteringStatsAction Request received on NEW PersistentTaskNode[{}]", persistentTaskNode2.getName());
                persistentTaskNode2Requests.incrementAndGet();
                logger.info(
                    "GetMeteringStatsAction start handling messageReceived on NEW PersistentTaskNode[{}]",
                    persistentTaskNode2.getName()
                );
                handler.messageReceived(request, channel, task);
                logger.info(
                    "GetMeteringStatsAction finished handling messageReceived on NEW PersistentTaskNode[{}]",
                    persistentTaskNode2.getName()
                );
            });

        newTaskNodeReadyLatch.countDown();

        var result = listener.get();
        assertThat(result, notNullValue());
        assertThat(persistentTaskNode1Requests.get(), greaterThanOrEqualTo(1));

        if (persistentTaskNode2.equals(persistentTaskNode1)) {
            assertThat(persistentTaskNode2Requests.get(), is(0));
        } else {
            assertThat(persistentTaskNode2Requests.get(), greaterThanOrEqualTo(1));
        }
    }

    private DiscoveryNode getMeteringPersistentTaskAssignedNode() throws Exception {
        AtomicReference<DiscoveryNode> persistentTaskNode = new AtomicReference<>();
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = MeteringIndexInfoTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
            persistentTaskNode.set(clusterState.nodes().get(task.getAssignment().getExecutorNode()));
        });
        return persistentTaskNode.get();
    }
}
