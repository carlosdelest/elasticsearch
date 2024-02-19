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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.operator.OperatorPrivilegesViolation;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerlessOperatorOnlyRegistryTests extends ESTestCase {

    @Before
    public void setup() {}

    public void testCheckRestFull() throws Exception {
        RestHandler restHandler = mock(RestHandler.class);
        RestRequest restRequest = mock(RestRequest.class);
        RestChannel restChannel = mock(RestChannel.class);
        ServerlessOperatorOnlyRegistry registry = new ServerlessOperatorOnlyRegistry(Set.of());

        // no access at all is controlled outside of operator privileges - so we only assert this precondition
        when(restHandler.getServerlessScope()).thenReturn(null);
        expectThrows(ElasticsearchException.class, () -> registry.checkRest(restHandler, restRequest, restChannel));

        // by the time we get here, we know the user is not an operator, so fully restrict the request for internal scope
        when(restHandler.getServerlessScope()).thenReturn(Scope.INTERNAL);
        String path = randomAlphaOfLength(10);
        RestRequest.Method method = randomFrom(RestRequest.Method.values());
        when(restRequest.uri()).thenReturn(path);
        when(restRequest.method()).thenReturn(method);
        when(restChannel.newErrorBuilder()).thenReturn(XContentBuilder.builder(XContentType.JSON.xContent()));
        ArgumentCaptor<RestResponse> responseCapture = ArgumentCaptor.forClass(RestResponse.class);
        OperatorPrivilegesViolation violation = registry.checkRest(restHandler, restRequest, restChannel);
        verify(restChannel).sendResponse(responseCapture.capture());
        assertThat(responseCapture.getValue().status(), is(RestStatus.NOT_FOUND));
        String violationMessage = "Request for uri ["
            + path
            + "] with method ["
            + method
            + "] exists but is not available when running in serverless mode";
        assertEquals(violation.message(), violationMessage);
        assertThat(responseCapture.getValue().content().utf8ToString(), containsString(violationMessage));

        when(restHandler.getServerlessScope()).thenReturn(Scope.PUBLIC);
        assertThat(registry.checkRest(restHandler, restRequest, restChannel), nullValue());
    }

    public void testCheckRestPartial() {
        List<String> restrictedPaths = randomList(1, 100, () -> randomAlphaOfLengthBetween(10, 20));
        List<String> unRestrictedPaths = randomList(
            1,
            100,
            () -> randomValueOtherThanMany(restrictedPaths::contains, () -> randomAlphaOfLengthBetween(10, 20))
        );
        ServerlessOperatorOnlyRegistry registry = new ServerlessOperatorOnlyRegistry(Sets.newHashSet(restrictedPaths));

        // create a rest request that should be restricted
        String restrictedPath = randomFrom(restrictedPaths);
        FakeRestRequest.Builder restrictedRequestBuilder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        restrictedRequestBuilder.withPath(restrictedPath);
        RestRequest restrictedRequest = restrictedRequestBuilder.build();

        // create a rest request that should not be restricted
        String unRestrictedPath = randomFrom(unRestrictedPaths);
        FakeRestRequest.Builder unRestrictedRequestBuilder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        unRestrictedRequestBuilder.withPath(unRestrictedPath);
        RestRequest unRestrictedRequest = restrictedRequestBuilder.build();

        // find a random additional unrestricted path we can add to our handler.
        RestHandler.Route additionalPath = randomFrom(
            unRestrictedPaths.stream()
                .map(p -> RestHandler.Route.builder(randomFrom(RestRequest.Method.values()), p).build())
                .collect(Collectors.toList())
        );

        RestHandler restHandler = mock(RestHandler.class);
        when(restHandler.getServerlessScope()).thenReturn(Scope.PUBLIC);
        // unrestricted path
        List<RestHandler.Route> handlerRoutes = Arrays.asList(
            RestHandler.Route.builder(randomFrom(RestRequest.Method.values()), unRestrictedPath).build(),
            additionalPath
        );
        Randomness.shuffle(handlerRoutes);
        when(restHandler.routes()).thenReturn(handlerRoutes);
        OperatorPrivilegesViolation violation = registry.checkRest(restHandler, unRestrictedRequest, null);
        assertNull(violation);
        assertNull(unRestrictedRequest.param(RestRequest.PATH_RESTRICTED));

        // restricted path
        handlerRoutes = Arrays.asList(
            RestHandler.Route.builder(randomFrom(RestRequest.Method.values()), restrictedPath).build(),
            additionalPath
        );
        Randomness.shuffle(handlerRoutes);
        when(restHandler.routes()).thenReturn(handlerRoutes);
        violation = registry.checkRest(restHandler, unRestrictedRequest, null);
        assertNull(violation);
        assertThat(restrictedRequest.param(RestRequest.PATH_RESTRICTED), is("serverless"));
    }

}
