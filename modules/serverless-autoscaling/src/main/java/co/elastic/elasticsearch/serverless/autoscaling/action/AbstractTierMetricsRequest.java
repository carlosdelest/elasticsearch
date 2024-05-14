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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

abstract class AbstractTierMetricsRequest<Request extends MasterNodeRequest<Request>> extends MasterNodeRequest<Request> {

    private final String tierName;
    private final TimeValue requestTimeout;

    AbstractTierMetricsRequest(final String tierName, final TimeValue masterNodeTimeout, final TimeValue requestTimeout) {
        super(masterNodeTimeout);
        this.requestTimeout = requestTimeout;
        assert Strings.isNullOrEmpty(tierName) == false;
        this.tierName = tierName;
    }

    AbstractTierMetricsRequest(final String tierName, final StreamInput in) throws IOException {
        super(in);
        this.requestTimeout = in.readTimeValue();
        this.tierName = tierName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(requestTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, format("get_serverless_" + tierName + "_tier_metrics"), parentTaskId, headers);
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }
}
