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

package co.elastic.elasticsearch.serverless.security.logging;

import co.elastic.elasticsearch.serverless.security.operator.ServerlessOperatorOnlyRegistry;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class ServerlessUserLoggingIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "test_operator";
    private static final String REGULAR_USER_ID = "test_user";

    private static final String REGULAR_USER_ROLE = "superuser";
    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        // ServerlessOperatorOnlyRegistry logs each non-operator request at trace level. Use this to trigger a log entry
        .setting("logger." + ServerlessOperatorOnlyRegistry.class.getName(), "TRACE")
        .user(OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(REGULAR_USER_ID, TEST_PASSWORD, REGULAR_USER_ROLE, false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String auth = basicAuthHeaderValue(REGULAR_USER_ID, TEST_PASSWORD_SECURE_STRING);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", auth).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String auth = basicAuthHeaderValue(OPERATOR_USER, TEST_PASSWORD_SECURE_STRING);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", auth).build();
    }

    public void testUserInfoIsLogged() throws Exception {
        client().performRequest(new Request("GET", "/"));

        // logging is not instantaneous. busy-loop until the required log entry exists
        final AtomicReference<String> logLine = new AtomicReference<>();
        assertBusy(() -> {
            try (InputStream log = cluster.getNodeLog(0, LogType.SERVER_JSON)) {
                final List<String> lines = Streams.readAllLines(log);
                final Optional<String> line = lines.stream()
                    .filter(s -> s.contains(ServerlessOperatorOnlyRegistry.class.getSimpleName()))
                    .findFirst();
                assertTrue(line.isPresent());
                logLine.set(line.get());
            }
        }, 5, TimeUnit.SECONDS);

        assertThat(logLine.get(), notNullValue());
        assertThat(logLine.get(), containsString("\"user.name\":\"" + REGULAR_USER_ID + "\""));
        assertThat(logLine.get(), containsString("\"auth.type\":\"REALM\""));
    }
}
