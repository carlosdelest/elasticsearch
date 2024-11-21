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

package co.elastic.elasticsearch.metering.usagereports.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class UpdateSampledMetricsMetadataAction {

    public static final String NAME = "cluster:monitor/update/metering/metadata";
    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>(NAME);

    public static class Request extends MasterNodeRequest<Request> {

        private final SampledMetricsMetadata metadata;

        public Request(TimeValue masterNodeTimeout, SampledMetricsMetadata metadata) {
            super(masterNodeTimeout);
            this.metadata = metadata;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.metadata = new SampledMetricsMetadata(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            metadata.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public SampledMetricsMetadata getMetadata() {
            return metadata;
        }
    }
}
