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

import co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IndexTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchTierMetrics;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetAutoscalingMetricsAction {

    public static final String NAME = "cluster:admin/serverless/autoscaling/get_serverless_autoscaling_metrics";
    public static final ActionType<Response> INSTANCE = ActionType.localOnly(NAME);

    private GetAutoscalingMetricsAction() {/* no instances */}

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
            return new CancellableTask(id, type, action, "get_serverless_autoscaling_metrics", parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        @Nullable
        private final IndexTierMetrics indexTierMetrics;
        @Nullable
        private final SearchTierMetrics searchTierMetrics;
        @Nullable
        private final MachineLearningTierMetrics machineLearningTierMetrics;

        public Response(
            @Nullable IndexTierMetrics indexTierMetrics,
            @Nullable SearchTierMetrics searchTierMetrics,
            @Nullable MachineLearningTierMetrics machineLearningTierMetrics
        ) {
            this.indexTierMetrics = indexTierMetrics;
            this.searchTierMetrics = searchTierMetrics;
            this.machineLearningTierMetrics = machineLearningTierMetrics;
        }

        public Response(final StreamInput input) throws IOException {
            super(input);
            indexTierMetrics = input.readOptionalWriteable(IndexTierMetrics::new);
            searchTierMetrics = input.readOptionalWriteable(SearchTierMetrics::new);
            machineLearningTierMetrics = input.readOptionalWriteable(MachineLearningTierMetrics::new);
        }

        IndexTierMetrics getIndexTierMetrics() {
            return indexTierMetrics;
        }

        SearchTierMetrics getSearchTierMetrics() {
            return searchTierMetrics;
        }

        MachineLearningTierMetrics getMachineLearningTierMetrics() {
            return machineLearningTierMetrics;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(indexTierMetrics);
            out.writeOptionalWriteable(searchTierMetrics);
            out.writeOptionalWriteable(machineLearningTierMetrics);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (indexTierMetrics != null) {
                builder.field("index", indexTierMetrics);
            }
            if (searchTierMetrics != null) {
                builder.field("search", searchTierMetrics);
            }
            if (machineLearningTierMetrics != null) {
                builder.field("ml", machineLearningTierMetrics);
            }
            builder.endObject();
            return builder;
        }

        public int hashCode() {
            return Objects.hash(indexTierMetrics, searchTierMetrics, machineLearningTierMetrics);
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
            return Objects.equals(indexTierMetrics, other.indexTierMetrics)
                && Objects.equals(searchTierMetrics, other.searchTierMetrics)
                && Objects.equals(machineLearningTierMetrics, other.machineLearningTierMetrics);
        }
    }
}
