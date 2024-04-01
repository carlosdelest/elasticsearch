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
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

/*
 * This class replaces the node part of the taskId in the GetTaskResponse with a pseudo node named "serverless". This is to avoid leaking
 *  information about underlying odes to users when running in serverless mode.
 */
public class TaskResponseFilter extends ApiFilteringActionFilter<GetTaskResponse> {
    protected TaskResponseFilter(ThreadContext threadContext) {
        super(threadContext, TransportGetTaskAction.TYPE.name(), GetTaskResponse.class);
    }

    @Override
    protected GetTaskResponse filterResponse(GetTaskResponse response) throws Exception {
        TaskResult originalTaskResult = response.getTask();
        TaskInfo originalTaskInfo = originalTaskResult.getTask();
        TaskInfo newTaskInfo = new TaskInfo(
            originalTaskInfo.taskId(),
            originalTaskInfo.type(),
            "serverless",
            originalTaskInfo.action(),
            originalTaskInfo.description(),
            originalTaskInfo.status(),
            originalTaskInfo.startTime(),
            originalTaskInfo.runningTimeNanos(),
            originalTaskInfo.cancellable(),
            originalTaskInfo.cancelled(),
            originalTaskInfo.parentTaskId(),
            originalTaskInfo.headers()
        );
        TaskResult taskResult = new TaskResult(
            originalTaskResult.isCompleted(),
            newTaskInfo,
            originalTaskResult.getError(),
            originalTaskResult.getResponse()
        );
        return new GetTaskResponse(taskResult);
    }
}
