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

package co.elastic.elasticsearch.serverless.security.apikey;

import co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin;
import co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleParser;
import co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidator;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilderFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class ServerlessCreateApiKeyRequestBuilderFactory implements CreateApiKeyRequestBuilderFactory {
    private final Supplier<Boolean> strictRequestValidationEnabled;

    // Needed for java module
    public ServerlessCreateApiKeyRequestBuilderFactory() {
        this(() -> false);
    }

    // For SPI
    public ServerlessCreateApiKeyRequestBuilderFactory(ServerlessSecurityPlugin plugin) {
        this(plugin::strictApiKeyRequestValidationEnabled);
    }

    private ServerlessCreateApiKeyRequestBuilderFactory(Supplier<Boolean> strictRequestValidationEnabled) {
        this.strictRequestValidationEnabled = strictRequestValidationEnabled;
    }

    @Override
    public CreateApiKeyRequestBuilder create(Client client, boolean restrictRequest) {
        return new ServerlessCreateApiKeyRequestBuilder(client, restrictRequest, strictRequestValidationEnabled);
    }

    static class ServerlessCreateApiKeyRequestBuilder extends CreateApiKeyRequestBuilder {
        private static final ConstructingObjectParser<CreateApiKeyRequest, Void> PARSER = createParser(
            ServerlessCustomRoleParser::parseWithWorkflowRestrictionAllowed
        );
        private final boolean restrictRequest;
        private final ServerlessCustomRoleValidator serverlessCustomRoleValidator;
        private final Supplier<Boolean> strictRequestValidationEnabled;

        ServerlessCreateApiKeyRequestBuilder(Client client, boolean restrictRequest, Supplier<Boolean> strictRequestValidationEnabled) {
            super(client);
            this.restrictRequest = restrictRequest;
            this.serverlessCustomRoleValidator = new ServerlessCustomRoleValidator();
            this.strictRequestValidationEnabled = strictRequestValidationEnabled;
        }

        @Override
        public CreateApiKeyRequest parse(BytesReference source, XContentType xContentType) throws IOException {
            if (false == restrictRequest) {
                return super.parse(source, xContentType);
            }

            final SourceWithXContentType sourceWithXContentType = new SourceWithXContentType(source, xContentType);
            if (strictRequestValidationEnabled.get()) {
                try {
                    return parseWithValidation(sourceWithXContentType);
                } catch (Exception ex) {
                    ServerlessCustomRoleErrorLogger.logException("API key creation request", ex);
                    throw ex;
                }
            }

            final CreateApiKeyRequest createApiKeyRequest = super.parse(source, xContentType);
            ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
                "API key creation request with ID [" + createApiKeyRequest.getId() + "]",
                sourceWithXContentType,
                this::parseWithValidation,
                createApiKeyRequest.getRoleDescriptors()
            );

            return createApiKeyRequest;
        }

        record SourceWithXContentType(BytesReference source, XContentType xContentType) {}

        private CreateApiKeyRequest parseWithValidation(SourceWithXContentType sourceWithXContentType) throws IOException {
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                    sourceWithXContentType.source(),
                    sourceWithXContentType.xContentType()
                )
            ) {
                final CreateApiKeyRequest createApiKeyRequest = PARSER.parse(parser, null);
                serverlessCustomRoleValidator.validateAndThrow(createApiKeyRequest.getRoleDescriptors(), false);
                return createApiKeyRequest;
            }
        }
    }
}
