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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class CloudApiKeyAuthenticationResponseTests extends ESTestCase {

    public void testParseValidResponse() throws Exception {
        final String apiKeyId = "cloud_api_key_id_" + randomAlphanumericOfLength(5);
        final String organizationId = "organization_id_" + randomAlphanumericOfLength(5);
        final List<String> roles = randomList(0, 3, () -> "role_" + randomAlphaOfLength(5));
        final String type = randomBoolean() ? null : "api_key";
        final String description = randomBoolean() ? null : "description_" + randomAlphaOfLength(5);

        final Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("api_key_id", apiKeyId);
        responseMap.put("organization_id", organizationId);
        responseMap.put("application_roles", roles);
        if (type != null) {
            responseMap.put("type", type);
        }
        if (description != null) {
            responseMap.put("api_key_description", description);
        }
        if (randomBoolean()) {
            responseMap.put("unknown_field_that_should_be_ignored", randomBoolean());
        }

        final String responseJson = mapToJsonString(responseMap);
        var result = CloudApiKeyAuthenticationResponse.parse(responseJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(apiKeyId, result.apiKeyId());
        assertEquals(organizationId, result.organizationId());
        assertEquals(roles, result.applicationRoles());
        assertEquals(type, result.type());
        assertEquals(description, result.apiKeyDescription());
    }

    public void testParseFailsOnMissingOrInvalidFields() throws Exception {
        // missing organization ID should fail parsing
        assertParsingFails(
            "Required [organization_id]",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("application_roles", randomValidApplicationRoles())
                )
            )
        );

        // missing API key ID should fail
        assertParsingFails(
            "Required [api_key_id]",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    Map.entry("application_roles", randomValidApplicationRoles())
                )
            )
        );

        // empty or blank API key ID should fail
        assertParsingFails(
            "api_key_id must not be null or empty",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", randomBoolean() ? "" : " "),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    Map.entry("application_roles", randomList(0, 3, () -> "role_" + randomAlphaOfLength(5)))
                )
            )
        );

        // empty or blank organization ID should fail
        assertParsingFails(
            "organization_id must not be null or empty",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("organization_id", randomBoolean() ? "" : " "),
                    Map.entry("application_roles", randomList(0, 3, () -> "role_" + randomAlphaOfLength(5)))
                )
            )
        );

        // application_roles are required
        assertParsingFails(
            "Required [application_roles]",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5))
                )
            )
        );
        // application_roles cannot contain null elements
        assertParsingFails(
            "found VALUE_NULL",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    Map.entry("application_roles", randomList(1, 3, () -> null))
                )
            )
        );
    }

    private String mapToJsonString(Map<String, ?> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static List<String> randomValidApplicationRoles() {
        // TODO: empty roles should be unexpected and invalid once the UIAM implements the validation and returns a 403
        return randomList(0, 3, () -> "role_" + randomAlphaOfLength(5));
    }

    private static void assertParsingFails(String expectedMessage, String responseJson) {
        var e = expectThrows(Exception.class, () -> CloudApiKeyAuthenticationResponse.parse(responseJson.getBytes(StandardCharsets.UTF_8)));
        if (e.getCause() != null) {
            assertThat(e.getCause().getMessage(), containsString(expectedMessage));
        } else {
            assertThat(e.getMessage(), containsString(expectedMessage));
        }
    }
}
