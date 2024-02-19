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

package co.elastic.elasticsearch.serverless.security.role;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.Map;

public class ServerlessCustomRoleParser {

    private ServerlessCustomRoleParser() {}

    public static RoleDescriptor parse(String name, BytesReference source, XContentType xContentType) throws IOException {
        assert name != null;
        try (XContentParser parser = createParser(source, xContentType)) {
            return parse(name, parser);
        }
    }

    private static RoleDescriptor parse(String name, XContentParser parser) throws IOException {
        // validate name
        Validation.Error validationError = Validation.Roles.validateRoleName(name, false);
        if (validationError != null) {
            ValidationException ve = new ValidationException();
            ve.addValidationError(validationError.toString());
            throw ve;
        }

        // advance to the START_OBJECT token if needed
        XContentParser.Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse role [{}]. expected an object but found [{}] instead", name, token);
        }
        String currentFieldName = null;
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges = null;
        String[] clusterPrivileges = null;
        RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = null;
        Map<String, Object> metadata = null;
        String[] runAsUsers = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (RoleDescriptor.Fields.INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                indicesPrivileges = RoleDescriptor.parseIndices(name, parser, false);
            } else if (RoleDescriptor.Fields.RUN_AS.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    runAsUsers = readStringArray(name, parser);
                    // Skip any run-as parsing exception -- the field must be absent or an empty array (for BWC) so any parsing errors we
                    // get are irrelevant and should not bubble up
                } catch (ElasticsearchParseException parseException) {
                    throw new ElasticsearchParseException(
                        "failed to parse role [{}]. In serverless mode run_as must be absent or empty.",
                        name
                    );
                }
                if (runAsUsers != null && runAsUsers.length > 0) {
                    throw new ElasticsearchParseException(
                        "failed to parse role [{}]. In serverless mode run_as must be absent or empty.",
                        name
                    );
                }
            } else if (RoleDescriptor.Fields.CLUSTER.match(currentFieldName, parser.getDeprecationHandler())) {
                clusterPrivileges = readStringArray(name, parser);
            } else if (RoleDescriptor.Fields.APPLICATIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                applicationPrivileges = RoleDescriptor.parseApplicationPrivileges(name, parser);
            } else if (RoleDescriptor.Fields.GLOBAL.match(currentFieldName, parser.getDeprecationHandler())) {
                throw new ElasticsearchParseException(
                    "failed to parse role [{}]. field [{}] is not supported when running in serverless mode",
                    name,
                    currentFieldName
                );
            } else if (RoleDescriptor.Fields.METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException(
                        "expected field [{}] to be of type object, but found [{}] instead",
                        currentFieldName,
                        token
                    );
                }
                metadata = parser.map();
            } else if (RoleDescriptor.Fields.TRANSIENT_METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    // consume object but just drop
                    parser.map();
                } else {
                    throw new ElasticsearchParseException("failed to parse role [{}]. unexpected field [{}]", name, currentFieldName);
                }
            } else if (RoleDescriptor.Fields.REMOTE_INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                throw new ElasticsearchParseException(
                    "failed to parse role [{}]. field [{}] is not supported when running in serverless mode",
                    name,
                    currentFieldName
                );
            } else if (RoleDescriptor.Fields.TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                // don't need it
            } else {
                throw new ElasticsearchParseException("failed to parse role [{}]. unexpected field [{}]", name, currentFieldName);
            }
        }
        return new RoleDescriptor(
            name,
            clusterPrivileges,
            indicesPrivileges,
            applicationPrivileges,
            null,
            null,
            metadata,
            null,
            null,
            null
        );
    }

    private static String[] readStringArray(String roleName, XContentParser parser) throws IOException {
        try {
            return XContentUtils.readStringArray(parser, true);
        } catch (ElasticsearchParseException e) {
            // re-wrap in order to add the role name
            throw new ElasticsearchParseException("failed to parse role [{}]", e, roleName);
        }
    }

    private static XContentParser createParser(BytesReference source, XContentType xContentType) throws IOException {
        return XContentHelper.createParserNotCompressed(LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG, source, xContentType);
    }
}
