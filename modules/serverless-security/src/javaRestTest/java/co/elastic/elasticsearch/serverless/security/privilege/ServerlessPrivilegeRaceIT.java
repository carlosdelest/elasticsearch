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

package co.elastic.elasticsearch.serverless.security.privilege;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServerlessPrivilegeRaceIT extends ESRestTestCase {

    protected static final String TEST_OPERATOR_USER = "elastic-operator-user";
    protected static final String TEST_PASSWORD = "elastic-password";

    private final int concurrentGetRequests = 5;
    private final ExecutorService executor = Executors.newFixedThreadPool(concurrentGetRequests + 1);

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        // Pick a very long timeout to account for slow CI machines
        .systemProperty("es.security.security_index.wait_timeout", "20s")
        .user(TEST_OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @After
    public void shutdownExecutor() {
        executor.shutdown();
    }

    public void testGetAndPutPrivilegeRace() throws ExecutionException, InterruptedException, TimeoutException {
        assertSecurityIndexNotFound();

        final List<Exception> exceptions = new CopyOnWriteArrayList<>();

        final List<Future<Void>> getFutures = new ArrayList<>();
        for (int i = 0; i < concurrentGetRequests; i++) {
            final Future<Void> future = executor.submit(() -> {
                try {
                    getPrivileges();
                } catch (Exception e) {
                    exceptions.add(e);
                }
                return null;
            });
            getFutures.add(future);
        }

        final Future<Void> putFuture = executor.submit(() -> {
            try {
                putPrivileges();
            } catch (Exception e) {
                exceptions.add(e);
            }
            return null;
        });

        putFuture.get(20, TimeUnit.SECONDS);
        for (Future<Void> future : getFutures) {
            future.get(20, TimeUnit.SECONDS);
        }

        assertThat(exceptions, is(empty()));
    }

    private static void assertSecurityIndexNotFound() {
        final Request request = new Request("GET", "/.security-7");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertThat(
            expectThrows(ResponseException.class, () -> adminClient().performRequest(request)).getResponse()
                .getStatusLine()
                .getStatusCode(),
            equalTo(404)
        );
    }

    private void putPrivileges() throws IOException {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity("""
            {
               "myapp": {
                 "read": {
                   "actions": [
                     "data:read/*",
                     "action:login" ],
                     "metadata": {
                       "description": "Read access to myapp"
                     }
                   }
                 }
             }""");
        final Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    private void getPrivileges() throws IOException {
        // Keep running requests until the first non-empty successful request is encountered
        // Failures such as 503s will be collected as exceptions and also stop the loop
        // Assertions will fail fast
        while (true) {
            final Request request = new Request("GET", "/_security/privilege/my*");
            final Response response = adminClient().performRequest(request);
            assertOK(response);
            final Map<String, Object> responseAsMap = responseAsMap(response);
            if (false == responseAsMap.isEmpty()) {
                assertThat(responseAsMap.keySet(), contains("myapp"));
                return;
            }
        }
    }
}
