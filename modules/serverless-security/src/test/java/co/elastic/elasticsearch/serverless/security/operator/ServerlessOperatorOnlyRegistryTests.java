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

package co.elastic.elasticsearch.serverless.security.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.ApiNotAvailableException;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerlessOperatorOnlyRegistryTests extends ESTestCase {

    @Before
    public void setup() {}

    public void testCheckRestFull() {
        RestHandler restHandler = mock(RestHandler.class);
        RestRequest restRequest = mock(RestRequest.class);
        ServerlessOperatorOnlyRegistry registry = new ServerlessOperatorOnlyRegistry();

        // no access at all is controlled outside of operator privileges - so we only assert this precondition
        when(restHandler.getServerlessScope()).thenReturn(null);
        expectThrows(ElasticsearchException.class, () -> registry.checkRest(restHandler, restRequest));

        // by the time we get here, we know the user is not an operator, so fully restrict the request for internal scope
        when(restHandler.getServerlessScope()).thenReturn(Scope.INTERNAL);
        String path = randomAlphaOfLength(10);
        RestRequest.Method method = randomFrom(RestRequest.Method.values());
        when(restRequest.uri()).thenReturn(path);
        when(restRequest.method()).thenReturn(method);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> registry.checkRest(restHandler, restRequest));
        assertThat(ex, instanceOf(ApiNotAvailableException.class));
        assertThat(ex.status(), is(RestStatus.GONE));
        String violationMessage = "Request for uri ["
            + path
            + "] with method ["
            + method
            + "] exists but is not available when running in serverless mode";
        assertThat(ex, throwableWithMessage(violationMessage));

        when(restHandler.getServerlessScope()).thenReturn(Scope.PUBLIC);
        try {
            registry.checkRest(restHandler, restRequest);
        } catch (ElasticsearchStatusException e) {
            fail("Public rest handlers should not trigger exceptions in the operator-only registry - " + e);
        }
    }
}
