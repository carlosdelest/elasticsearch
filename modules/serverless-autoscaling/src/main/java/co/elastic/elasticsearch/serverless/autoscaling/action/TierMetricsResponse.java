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

import co.elastic.elasticsearch.serverless.autoscaling.model.TierMetrics;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

class TierMetricsResponse extends ActionResponse implements ToXContentFragment {

    private final String tierName;
    private final TierMetrics tierMetrics;

    TierMetricsResponse(final String tierName, final TierMetrics tierMetrics) {
        this.tierName = tierName;
        this.tierMetrics = tierMetrics;
    }

    TierMetricsResponse(final StreamInput input) throws IOException {
        super(input);
        this.tierName = input.readString();
        this.tierMetrics = new TierMetrics(input);
    }

    public String getTierName() {
        return tierName;
    }

    public TierMetrics getTierMetrics() {
        return tierMetrics;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierName, tierMetrics);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof TierMetricsResponse == false) {
            return false;
        }
        TierMetricsResponse other = (TierMetricsResponse) obj;
        return Objects.equals(tierName, other.tierName) && Objects.equals(tierMetrics, other.tierMetrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder.field(tierName, tierMetrics);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tierName);
        tierMetrics.writeTo(out);
    }
}
