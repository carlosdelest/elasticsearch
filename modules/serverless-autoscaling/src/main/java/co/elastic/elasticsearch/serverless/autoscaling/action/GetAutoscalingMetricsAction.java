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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction.Response;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class GetAutoscalingMetricsAction extends ActionType<Response> {

    public static final GetAutoscalingMetricsAction INSTANCE = new GetAutoscalingMetricsAction();
    public static final String NAME = "cluster:admin/serverless/autoscaling/get_serverless_autoscaling_metrics";

    public GetAutoscalingMetricsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public Request(TimeValue timeout) {
            super(timeout);
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("get_serverless_autoscaling_metrics"), parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<TierMetricsResponse> tierResponses;

        public Response(List<TierMetricsResponse> tierResponses) {
            this.tierResponses = Collections.unmodifiableList(tierResponses);
        }

        public Response(final StreamInput input) throws IOException {
            super(input);
            this.tierResponses = input.readImmutableList(TierMetricsResponse::new);
        }

        // for testing
        List<TierMetricsResponse> getTierResponses() {
            return tierResponses;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tierResponses);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj instanceof Response == false) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(tierResponses, other.tierResponses);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (TierMetricsResponse tierResponse : tierResponses) {
                tierResponse.toXContent(builder, params);
            }

            builder.field("took", 42);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(tierResponses);
        }
    }
}
