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

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.APPLICATION_ROLES_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_ID_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_ORGANIZATION_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_TYPE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record CloudAuthenticateProjectContext(ProjectInfo project, List<String> applicationRoles) {

    private static final ConstructingObjectParser<CloudAuthenticateProjectContext, Void> PARSER = buildParser();

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<CloudAuthenticateProjectContext, Void> buildParser() {
        final ConstructingObjectParser<CloudAuthenticateProjectContext, Void> parser = new ConstructingObjectParser<>(
            "contexts",
            true,
            a -> {
                int i = 0;
                ProjectInfo project = new ProjectInfo(
                    (String) a[i++], // projectId
                    (String) a[i++], // organizationId
                    (String) a[i++]  // projectType
                );
                return new CloudAuthenticateProjectContext(
                    project,
                    (List<String>) a[i++] // applicationRoles
                );
            }
        );
        parser.declareString(constructorArg(), new ParseField(PROJECT_ID_FIELD));
        parser.declareString(constructorArg(), new ParseField(PROJECT_ORGANIZATION_FIELD));
        parser.declareString(constructorArg(), new ParseField(PROJECT_TYPE_FIELD));
        parser.declareStringArray(constructorArg(), new ParseField(APPLICATION_ROLES_FIELD));
        return parser;
    }

    public static CloudAuthenticateProjectContext parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
