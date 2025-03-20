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

package co.elastic.elasticsearch.serverless.multiproject.action;

import co.elastic.elasticsearch.serverless.multiproject.action.TransportGetProjectStatusAction.Response;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GetProjectStatusResponseSerializationTests extends AbstractXContentSerializingTestCase<Response> {

    private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
        "get_project_status_response",
        objects -> new Response((String) objects[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Response.PROJECT_ID);
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomUUID());
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        return new Response(randomValueOtherThan(instance.getProjectId(), ESTestCase::randomUUID));
    }
}
