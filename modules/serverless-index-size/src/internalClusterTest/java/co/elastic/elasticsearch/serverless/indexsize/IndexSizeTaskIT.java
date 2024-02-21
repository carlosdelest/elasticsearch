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

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class IndexSizeTaskIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(IndexSizePlugin.class)).collect(Collectors.toSet());
    }

    @After
    public void cleanUp() {
        updateClusterSettings(
            Settings.builder()
                .putNull(IndexSizeTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(IndexSizeTaskExecutor.POLL_INTERVAL_SETTING.getKey())
        );
    }

    public void testTaskRemovedAfterCancellation() throws Exception {
        updateClusterSettings(Settings.builder().put(IndexSizeTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            var task = IndexSizeTask.findTask(clusterService().state());
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().listTasks(new ListTasksRequest().setActions("index-size[c]")).actionGet();
            assertThat(tasks.getTasks(), hasSize(1));
        });
        updateClusterSettings(Settings.builder().put(IndexSizeTaskExecutor.ENABLED_SETTING.getKey(), false));
        assertBusy(() -> {
            ListTasksResponse tasks2 = clusterAdmin().listTasks(new ListTasksRequest().setActions("index-size[c]")).actionGet();
            assertThat(tasks2.getTasks(), empty());
        });
    }
}
