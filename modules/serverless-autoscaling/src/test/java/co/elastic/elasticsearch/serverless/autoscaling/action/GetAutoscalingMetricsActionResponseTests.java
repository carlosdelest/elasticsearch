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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetricsTests.randomMachineLearningTierMetrics;
import static co.elastic.elasticsearch.serverless.autoscaling.action.GetIndexTierMetricsSerializationTests.randomIndexTierMetrics;

public class GetAutoscalingMetricsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    public static Response randomAutoscalingMetricsResponse() {
        return
        // TODO: search tier has no serialization tests yet
        new Response(randomIndexTierMetrics(), null, randomMachineLearningTierMetrics());
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return randomAutoscalingMetricsResponse();
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        int branch = randomInt(1);
        return switch (branch) {
            case 0 -> new Response(randomIndexTierMetrics(), instance.getSearchTierMetrics(), instance.getMachineLearningTierMetrics());
            case 1 -> new Response(instance.getIndexTierMetrics(), instance.getSearchTierMetrics(), randomMachineLearningTierMetrics());
            default -> throw new IllegalStateException("Unexpected value: " + branch);
        };
    }
}
