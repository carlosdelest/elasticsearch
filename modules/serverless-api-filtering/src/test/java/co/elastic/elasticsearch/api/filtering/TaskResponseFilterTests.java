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

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import static org.hamcrest.Matchers.equalTo;

public class TaskResponseFilterTests extends ESTestCase {
    public void testFilterTasks() throws Exception {
        TaskResponseFilter taskResponseFilter = new TaskResponseFilter(new ThreadContext(Settings.EMPTY));
        String nodeId = randomAlphaOfLength(20);
        TaskId taskId = new TaskId(nodeId, randomLong());
        String parentNodeId = randomAlphaOfLength(20);
        TaskId parentTaskId = new TaskId(parentNodeId, randomLong());
        TaskInfo taskInfo = new TaskInfo(
            taskId,
            randomAlphaOfLength(20),
            nodeId,
            randomAlphaOfLength(20),
            randomAlphaOfLength(20),
            new Task.Status() {
                @Override
                public String getWriteableName() {
                    return null;
                }

                @Override
                public void writeTo(StreamOutput out) {}

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                    return null;
                }
            },
            randomLong(),
            randomLong(),
            randomBoolean(),
            false,
            parentTaskId,
            randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(20), randomAlphaOfLength(20)))
        );
        TaskResult taskResult = new TaskResult(
            randomBoolean(),
            taskInfo,
            new BytesArray(randomAlphaOfLength(20)),
            new BytesArray(randomAlphaOfLength(20))
        );
        GetTaskResponse original = new GetTaskResponse(taskResult);
        GetTaskResponse modified = taskResponseFilter.filterResponse(original);

        assertThat(modified.getTask().getTask().node(), equalTo("serverless"));
        assertThat(original.getTask().getTask().node(), equalTo(nodeId));

        // Now just making sure that a few of the other fields are unchanged (not an exhaustive list):
        assertThat(modified.getTask().getTask().taskId(), equalTo(original.getTask().getTask().taskId()));
        assertThat(modified.getTask().isCompleted(), equalTo(original.getTask().isCompleted()));
        assertThat(modified.getTask().getError(), equalTo(original.getTask().getError()));
        assertThat(modified.getTask().getResponse(), equalTo(original.getTask().getResponse()));
        assertThat(modified.getTask().getTask().type(), equalTo(original.getTask().getTask().type()));
        assertThat(modified.getTask().getTask().action(), equalTo(original.getTask().getTask().action()));
        assertThat(modified.getTask().getTask().parentTaskId(), equalTo(original.getTask().getTask().parentTaskId()));
    }
}
