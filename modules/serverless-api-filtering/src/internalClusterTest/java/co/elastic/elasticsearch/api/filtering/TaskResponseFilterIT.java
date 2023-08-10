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

package co.elastic.elasticsearch.api.filtering;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;

import static org.elasticsearch.test.rest.ESRestTestCase.assertOKAndCreateObjectPath;

public class TaskResponseFilterIT extends ESIntegTestCase {

    @Override
    public boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), ServerlessApiFilteringPlugin.class);
    }

    public void testGetTaskApiForNonOperator() throws Exception {
        final var transportService = internalCluster().getInstance(TransportService.class, internalCluster().getRandomNodeName());
        final var taskManager = transportService.getTaskManager();
        final var task = taskManager.register("test", "action", TransportRequest.Empty.INSTANCE);
        try {
            assertEquals(
                "serverless",
                assertOKAndCreateObjectPath(
                    getRestClient().performRequest(
                        new Request("GET", "/_tasks/" + new TaskId(transportService.getLocalNode().getId(), task.getId()))
                    )
                ).evaluate("task.node")
            );
        } finally {
            taskManager.unregister(task);
        }
    }
}
