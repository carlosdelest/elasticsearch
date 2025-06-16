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

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response of a successful cloud API key authentication against a serverless project.
 *
 * @param apiKeyId the ID of the cloud API key used for authentication.
 * @param organizationId the ID of the organization associated with the cloud API key.
 * @param applicationRoles the list of application roles granted to the cloud API key.
 * @param type the type of authentication, which can be null or a string indicating the type of authentication used (e.g. {@code api_key}).
 */
public record CloudApiKeyAuthenticationResponse(
    String apiKeyId,
    String organizationId,
    List<String> applicationRoles,
    @Nullable String type
) {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField API_KEY_ID_FIELD = new ParseField("api_key_id");
    private static final ParseField ORGANIZATION_ID_FIELD = new ParseField("organization_id");
    private static final ParseField APPLICATION_ROLES_FIELD = new ParseField("application_roles");

    private static final ConstructingObjectParser<CloudApiKeyAuthenticationResponse, Void> PARSER = buildParser();

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<CloudApiKeyAuthenticationResponse, Void> buildParser() {
        final ConstructingObjectParser<CloudApiKeyAuthenticationResponse, Void> parser = new ConstructingObjectParser<>(
            "authenticate_project_with_api_key_response",
            true,
            a -> new CloudApiKeyAuthenticationResponse((String) a[0], (String) a[1], (List<String>) a[2], (String) a[3])
        );
        parser.declareString(constructorArg(), API_KEY_ID_FIELD);
        parser.declareString(constructorArg(), ORGANIZATION_ID_FIELD);
        parser.declareStringArray(constructorArg(), APPLICATION_ROLES_FIELD);
        parser.declareString(optionalConstructorArg(), TYPE_FIELD);
        return parser;
    }

    public static CloudApiKeyAuthenticationResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ApiKeyProjectAuthenticationResponse{");
        sb.append("apiKeyId=[").append(apiKeyId).append(']');
        sb.append(", organizationId=[").append(organizationId).append(']');
        sb.append(", applicationRoles=").append(applicationRoles);
        sb.append(", type=[']").append(type).append(']');
        sb.append('}');
        return sb.toString();
    }
}
