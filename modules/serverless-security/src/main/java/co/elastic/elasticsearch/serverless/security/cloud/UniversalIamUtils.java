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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Utilities for universal IAM authentication.
 */
public final class UniversalIamUtils {

    /**
     * Creates a JSON parser with the default configuration.
     */
    public static XContentParser createJsonParser(byte[] data) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, data);
    }

    /**
     * Gets header value from thread context. The header name is case-insensitive.
     *
     * @param threadContext contains the request headers
     * @param headerName the name of the header to look for
     * @return the secure string value of the header or null if not found
     * @throws IllegalArgumentException if multiple values are found
     */
    public static SecureString getHeaderValue(final ThreadContext threadContext, final String headerName) {
        final List<String> headerValues = threadContext.getRequestHeadersOnly()
            .entrySet()
            .stream()
            .filter(e -> headerName.equalsIgnoreCase(e.getKey()))
            .filter(e -> Strings.hasText(e.getValue()))
            .map(Map.Entry::getValue)
            .toList();
        if (headerValues.isEmpty()) {
            return null;
        } else if (headerValues.size() == 1) {
            return new SecureString(headerValues.getFirst().toCharArray());
        } else {
            throw new IllegalArgumentException("received multiple values for single-valued header [" + headerName + "]");
        }
    }

}
