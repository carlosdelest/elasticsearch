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

package co.elastic.elasticsearch.serverless.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

public abstract class AbstractServerlessCustomRolesRestTestCase extends ESRestTestCase {
    protected static final String TEST_OPERATOR_USER = "elastic-operator-user";
    protected static final String TEST_USER = "elastic-user";
    protected static final String TEST_PASSWORD = "elastic-password";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return false;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected Response executeAsUser(String username, Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString(TEST_PASSWORD.toCharArray())))
        );
        return client().performRequest(request);
    }

    protected Response executeAndAssertSuccess(String username, Request request) throws IOException {
        final Response response = executeAsUser(username, request);
        assertOK(response);
        return response;
    }
}
