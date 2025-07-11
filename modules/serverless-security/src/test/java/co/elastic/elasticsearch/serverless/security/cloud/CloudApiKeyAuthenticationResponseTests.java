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
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
        responseMap.put(
            "contexts",
            List.of(
                Map.ofEntries(
                    Map.entry("application_roles", roles),
                    Map.entry("project_type", "elasticsearch"),
                    Map.entry("project_id", "1"),
                    Map.entry("project_organization_id", "1")
                )
            )
        );
        if (type != null) {
            responseMap.put("type", type);
        }
        if (description != null) {
            responseMap.put("api_key_description", description);
        }
        if (randomBoolean()) {
            responseMap.put("unknown_field_that_should_be_ignored", randomBoolean());
        }

        responseMap.put(
            "credentials",
            Map.ofEntries(
                Map.entry("type", "api-key"),
                Map.entry("creation", "1740107918000"),
                Map.entry("expiration", "1740107918000"),
                Map.entry("internal", randomBoolean())
            )
        );

        final String responseJson = mapToJsonString(responseMap);
        var result = CloudApiKeyAuthenticationResponse.parse(responseJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(apiKeyId, result.apiKeyId());
        assertEquals(organizationId, result.organizationId());
        assertThat(result.contexts().size(), equalTo(1));
        assertEquals(roles, result.contexts().getFirst().applicationRoles());
        assertEquals("elasticsearch", result.contexts().getFirst().project().projectType());
        assertEquals("1", result.contexts().getFirst().project().projectId());
        assertEquals("1", result.contexts().getFirst().project().organizationId());
        assertEquals(type, result.type());
        assertEquals(description, result.apiKeyDescription());
    }

    public void testParseFailsOnMissingOrInvalidFields() throws Exception {
        Map.Entry<String, Object> authContexts = Map.entry(
            "contexts",
            List.of(
                Map.ofEntries(
                    Map.entry("application_roles", randomValidApplicationRoles()),
                    Map.entry("project_type", "elasticsearch"),
                    Map.entry("project_id", "1"),
                    Map.entry("project_organization_id", "1")
                )
            )
        );
        Map.Entry<String, Object> credentialsMetadata = Map.entry(
            "credentials",
            Map.ofEntries(
                Map.entry("type", "api-key"),
                Map.entry("creation", "1740107918000"),
                Map.entry("expiration", "1740107918000"),
                Map.entry("internal", randomBoolean())
            )
        );
        // missing organization ID should fail parsing
        assertParsingFails(
            "Required [organization_id]",
            mapToJsonString(
                Map.ofEntries(Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)), credentialsMetadata, authContexts)
            )
        );

        // missing API key ID should fail
        assertParsingFails(
            "Required [api_key_id]",
            mapToJsonString(
                Map.ofEntries(Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)), credentialsMetadata, authContexts)
            )
        );

        // empty or blank API key ID should fail
        assertParsingFails(
            "api_key_id must not be null or empty",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", randomBoolean() ? "" : " "),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    credentialsMetadata,
                    authContexts
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
                    credentialsMetadata,
                    authContexts
                )
            )
        );

        // application_roles are required
        assertParsingFails(
            "Required [application_roles]",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    credentialsMetadata,
                    Map.entry(
                        "contexts",
                        List.of(
                            Map.ofEntries(
                                Map.entry("project_type", "elasticsearch"),
                                Map.entry("project_id", "1"),
                                Map.entry("project_organization_id", "1")
                            )
                        )
                    )
                )
            )
        );
        // application_roles cannot contain null elements
        assertParsingFails(
            "[contexts] failed to parse field [application_roles]",
            mapToJsonString(
                Map.ofEntries(
                    Map.entry("api_key_id", "api_key_id_" + randomAlphaOfLength(5)),
                    Map.entry("organization_id", "organization_id_" + randomAlphaOfLength(5)),
                    credentialsMetadata,
                    Map.entry(
                        "contexts",
                        List.of(
                            Map.ofEntries(
                                Map.entry("application_roles", randomList(1, 3, () -> null)),
                                Map.entry("project_type", "elasticsearch"),
                                Map.entry("project_id", "1"),
                                Map.entry("project_organization_id", "1")
                            )
                        )
                    )
                )
            )
        );
    }

    public void testParseWithMultipleContexts() throws IOException {
        final String json = """
            {
              "type": "api-key",
              "api_key_id": "V-04yYYB_35KQtWMCV5P",
              "api_key_description": "My universal API key",
              "organization_id": "1",
              "contexts": [
                {
                  "project_id": "1",
                  "project_organization_id": "1",
                  "project_type": "elasticsearch",
                  "application_roles": [
                    "developer",
                    "admin"
                  ]
                },
                {
                  "project_id": "2",
                  "project_organization_id": "2",
                  "project_type": "observability",
                  "application_roles": []
                }
              ],
              "credentials": {
                "type": "api-key",
                "creation": 1740107918000,
                "expiration": 1740107918000,
                "internal": true
              },
              "token": "<request-scoped token>"
            }
            """;
        var result = CloudApiKeyAuthenticationResponse.parse(json.getBytes(StandardCharsets.UTF_8));
        assertThat(result.type(), containsString("api-key"));
        assertThat(result.apiKeyId(), containsString("V-04yYYB_35KQtWMCV5P"));
        assertThat(result.apiKeyDescription(), containsString("My universal API key"));
        assertThat(result.organizationId(), containsString("1"));
        assertThat(result.contexts().size(), equalTo(2));
        assertThat(result.contexts().get(0).project().projectId(), containsString("1"));
        assertThat(result.contexts().get(0).project().organizationId(), containsString("1"));
        assertThat(result.contexts().get(0).project().projectType(), containsString("elasticsearch"));
        assertThat(result.contexts().get(0).applicationRoles(), containsInAnyOrder("admin", "developer"));
        assertThat(result.contexts().get(1).project().projectId(), containsString("2"));
        assertThat(result.contexts().get(1).project().organizationId(), containsString("2"));
        assertThat(result.contexts().get(1).project().projectType(), containsString("observability"));
        assertThat(result.contexts().get(1).applicationRoles(), is(empty()));
        assertThat(result.credentialsMetadata().type(), containsString("api-key"));
        assertThat(result.credentialsMetadata().internal(), equalTo(true));
        assertThat(result.credentialsMetadata().creation(), equalTo(Instant.ofEpochMilli(1740107918000L)));
        assertThat(result.credentialsMetadata().expiration(), equalTo(Instant.ofEpochMilli(1740107918000L)));
    }

    public void testParseWithoutCredentialsMetadata() throws IOException {
        final String json = """
            {
                "type": "api-key",
                "api_key_id": "82X1ppcBU1VJR-URtR5h",
                "organization_id": "1",
                "contexts": [
                    {
                        "type": "project",
                        "project_id": "1",
                        "project_type": "elasticsearch",
                        "project_organization_id": "1",
                        "application_roles": [
                            "superuser"
                        ]
                    }
                ]
            }
            """;
        var result = CloudApiKeyAuthenticationResponse.parse(json.getBytes(StandardCharsets.UTF_8));
        assertThat(result.type(), containsString("api-key"));
        assertThat(result.apiKeyId(), containsString("82X1ppcBU1VJR-URtR5h"));
        assertThat(result.organizationId(), containsString("1"));
        assertThat(result.contexts().size(), equalTo(1));
        assertThat(result.contexts().get(0).project().projectId(), containsString("1"));
        assertThat(result.contexts().get(0).project().organizationId(), containsString("1"));
        assertThat(result.contexts().get(0).project().projectType(), containsString("elasticsearch"));
        assertThat(result.contexts().get(0).applicationRoles(), containsInAnyOrder("superuser"));
        assertThat(result.credentialsMetadata(), is(nullValue()));
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
