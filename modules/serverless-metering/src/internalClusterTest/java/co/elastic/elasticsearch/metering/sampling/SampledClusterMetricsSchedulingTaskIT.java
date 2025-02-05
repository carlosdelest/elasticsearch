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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
            assertThat(task, isAssignedTask());
            assertThat(getSamplingTask(), isPresentWith(taskInfoOf(task)));
        });

        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), false));

        assertBusy(() -> {
            assertThat(SampledClusterMetricsSchedulingTask.findTask(clusterService().state()), nullValue());
            assertThat(getSamplingTask(), isEmpty());
        });

        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));

        assertBusy(() -> {
            var task = SampledClusterMetricsSchedulingTask.findTask(clusterService().state());
            assertThat(task, isAssignedTask());
            assertThat(getSamplingTask(), isPresentWith(taskInfoOf(task)));
        });

    }

    public void testTaskMoveToAnotherNode() throws Exception {
        updateClusterSettings(Settings.builder().put(SampledClusterMetricsSchedulingTaskExecutor.ENABLED_SETTING.getKey(), true));

        var persistentTaskNode1 = getMeteringPersistentTaskAssignedNode();

        AtomicLong oldTaskId = new AtomicLong();
        assertBusy(() -> {
            var task = getSamplingTask();
            assertThat(task, isPresent());
            oldTaskId.set(task.get().id());
        });

        assertTrue(internalCluster().stopNode(persistentTaskNode1.getName()));

        assertBusy(() -> assertThat(getSamplingTask(), isPresentWith(transformedMatch(TaskInfo::id, not(oldTaskId.get())))));

        assertBusy(() -> {
            // Verifying the PersistentTask runs on a new node
            var persistentTaskNode2 = getMeteringPersistentTaskAssignedNode();

            assertThat(persistentTaskNode2, notNullValue());
            assertThat(persistentTaskNode2.getName(), not(equalTo(persistentTaskNode1.getName())));
        });
    }

    private static Optional<TaskInfo> getSamplingTask() {
        return clusterAdmin().listTasks(new ListTasksRequest().setActions("metering-index-info[c]"))
            .actionGet()
            .getTasks()
            .stream()
            .findFirst();
    }

    private static Matcher<TaskInfo> taskInfoOf(PersistentTask<?> task) {
        return transformedMatch(t -> t.parentTaskId().getId(), equalTo(task.getAllocationId()));
    }

    private static Matcher<PersistentTask<?>> isAssignedTask() {
        return transformedMatch(t -> {
            assertThat(t, notNullValue());
            return t.isAssigned();
        }, equalTo(true));
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
