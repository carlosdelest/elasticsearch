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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

@LuceneTestCase.AwaitsFix(bugUrl = "ES-6252")
public class GetAutoscalingMetricsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    public static Response randomAutoscalingMetricsResponse() {
        return null;
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
        // TODO: Add serialization tests once all the APIs are stable enough
        return instance;
    }
}
