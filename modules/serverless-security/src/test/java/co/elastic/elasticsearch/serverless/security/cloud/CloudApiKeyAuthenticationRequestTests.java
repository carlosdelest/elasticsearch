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
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

public class CloudApiKeyAuthenticationRequestTests extends ESTestCase {

    public void testJsonSerialization() {
        SecureString apiKey = new SecureString(("essu_test_" + randomAlphanumericOfLength(10)).toCharArray());
        SecureString secret = randomBoolean() ? null : new SecureString(("shared_secret_" + randomAlphanumericOfLength(10)).toCharArray());
        CloudApiKeyAuthenticationRequest request = new CloudApiKeyAuthenticationRequest(
            List.of(new ProjectInfo("1234", "5678", "elasticsearch")),
            new CloudApiKey(apiKey, secret)
        );

        assertThat(Strings.toString(request, true, false), equalToCompressingWhiteSpace("""
            {
              "contexts" : [
                {
                  "type" : "project",
                  "project_id" : "1234",
                  "project_organization_id" : "5678",
                  "project_type" : "elasticsearch"
                }
              ]
            }
            """));
    }
}
