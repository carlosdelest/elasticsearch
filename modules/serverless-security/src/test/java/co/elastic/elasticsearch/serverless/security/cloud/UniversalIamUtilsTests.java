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

package co.elastic.elasticsearch.serverless.security.cloud;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import static co.elastic.elasticsearch.serverless.security.cloud.CloudApiKeyAuthenticator.CLIENT_AUTHENTICATION_HEADER;

public class UniversalIamUtilsTests extends ESTestCase {

    public void testGetHeaderValueIsCaseInsensitive() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        String headerName = randomFrom("x-client-authentication", "X-CLIENT-AUTHENTICATION", "X-client-Authentication");
        threadContext.putHeader(headerName, "secret");
        assertEquals(
            new SecureString("secret".toCharArray()),
            UniversalIamUtils.getHeaderValue(threadContext, CLIENT_AUTHENTICATION_HEADER)
        );
    }

    public void testGetHeaderValueNoHeader() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (randomBoolean()) {
            threadContext.putHeader("x-client-authentication", randomBoolean() ? null : "");
        }
        assertNull(UniversalIamUtils.getHeaderValue(threadContext, CLIENT_AUTHENTICATION_HEADER));
    }

    public void testGetHeaderValueMultipleHeaders() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("x-client-authentication", "secret1");
        threadContext.putHeader("X-CLIENT-AUTHENTICATION", "secret2");
        threadContext.putHeader("X-Client-Authentication", "secret3");
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> UniversalIamUtils.getHeaderValue(threadContext, CLIENT_AUTHENTICATION_HEADER)
        );
        assertEquals("received multiple values for single-valued header [" + CLIENT_AUTHENTICATION_HEADER + "]", e.getMessage());
    }
}
