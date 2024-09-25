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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.MeteringPlugin;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SampledClusterMetricsSchedulingTaskIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MeteringPlugin.class);
    }

    @Before
    public void setupCluster() {
        startMasterOnlyNode();
        startSearchNodes(2); // persistent task is assigned to search node
    }

    public void testTaskRemovedAfterCancellation() throws Exception {
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterService().state());
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().listTasks(new ListTasksRequest().setActions("metering-index-info[c]")).actionGet();
            assertThat(tasks.getTasks(), hasSize(1));
        });
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            ListTasksResponse tasks2 = clusterAdmin().listTasks(new ListTasksRequest().setActions("metering-index-info[c]")).actionGet();
            assertThat(tasks2.getTasks(), empty());
        });
    }

    public void testTaskMoveToAnotherNode() throws Exception {
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));

        var persistentTaskNode1 = getMeteringPersistentTaskAssignedNode();

        AtomicLong oldTaskId = new AtomicLong();
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().listTasks(new ListTasksRequest().setActions("metering-index-info[c]")).actionGet();
            assertThat(tasks.getTasks(), hasSize(1));

            oldTaskId.set(tasks.getTasks().get(0).id());
        });

        assertTrue(internalCluster().stopNode(persistentTaskNode1.getName()));

        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().listTasks(new ListTasksRequest().setActions("metering-index-info[c]")).actionGet();
            var taskIds = tasks.getTasks().stream().map(TaskInfo::id).collect(Collectors.toSet());
            assertFalse(taskIds.contains(oldTaskId.get()));
        });

        assertBusy(() -> {
            // Verifying the PersistentTask runs on a new node
            var persistentTaskNode2 = getMeteringPersistentTaskAssignedNode();

            assertThat(persistentTaskNode2, notNullValue());
            assertThat(persistentTaskNode2.getName(), not(equalTo(persistentTaskNode1.getName())));
        });
    }

    private DiscoveryNode getMeteringPersistentTaskAssignedNode() throws Exception {
        AtomicReference<DiscoveryNode> persistentTaskNode = new AtomicReference<>();
        assertBusy(() -> {
            var clusterState = clusterService().state();
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterState);
            assertNotNull(task);
            assertTrue(task.isAssigned());
            persistentTaskNode.set(clusterState.nodes().get(task.getAssignment().getExecutorNode()));
        });
        return persistentTaskNode.get();
    }
}
