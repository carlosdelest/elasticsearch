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

package co.elastic.elasticsearch.serverless.observability;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.util.Optional;

import static org.elasticsearch.test.cluster.local.model.User.DEFAULT_USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractEsqlVerifiersIT extends ESRestTestCase {

    protected abstract ObservabilityTier getObservabilityTier();

    public void testWithCategorize() {
        Optional<ResponseException> e = runEsql("""
            {
              "query": "ROW message=\\"foo bar\\" | STATS COUNT(*) BY CATEGORIZE(message) | LIMIT 1"
            }""");
        maybeVerifyException(e, "line 1:43: CATEGORIZE is unsupported");
    }

    public void testWithChangePoint() {
        Optional<ResponseException> e = runEsql(
            "{"
                + "      \"query\": \"ROW key=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25] | "
                + "MV_EXPAND key | "
                + "EVAL value = CASE(key<13, 0, 42) | "
                + "CHANGE_POINT value ON key | "
                + "WHERE type IS NOT NULL | "
                + "LIMIT 1\"\n"
                + "}"

        );
        maybeVerifyException(e, "line 1:130: CHANGE_POINT is unsupported");
    }

    private Optional<ResponseException> runEsql(String json) {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(json);
        request.addParameter("error_trace", "");
        request.addParameter("pretty", "");

        try {
            client().performRequest(request);
        } catch (Exception e) {
            assertThat(e, Matchers.instanceOf(ResponseException.class));
            return Optional.of((ResponseException) e);
        }

        return Optional.empty();
    }

    private void maybeVerifyException(Optional<ResponseException> exception, String expectedMessage) {
        switch (getObservabilityTier()) {
            case COMPLETE:
                assert exception.isEmpty() : "expected successful response";
                break;
            case LOGS_ESSENTIALS:
                assert exception.isPresent() : "expected response exception";
                exception.ifPresent(e -> {
                    assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                    assertThat(e.getMessage(), containsString(expectedMessage));
                });
                break;
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(DEFAULT_USER.getUsername(), new SecureString(DEFAULT_USER.getPassword().toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
