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

import co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleParser;
import co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class ServerlessCreateApiKeyRequestBuilderFactory implements CreateApiKeyRequestBuilderFactory {
    // Needed for java module
    public ServerlessCreateApiKeyRequestBuilderFactory() {}

    @Override
    public CreateApiKeyRequestBuilder create(Client client) {
        return new ServerlessCreateApiKeyRequestBuilder(client);
    }

    static class ServerlessCreateApiKeyRequestBuilder extends CreateApiKeyRequestBuilder {
        private static final Logger logger = LogManager.getLogger(ServerlessCreateApiKeyRequestBuilder.class);
        private static final ConstructingObjectParser<CreateApiKeyRequest, Void> PARSER = createParser(
            ServerlessCustomRoleParser::parseApiKeyRoleDescriptor
        );
        private final ServerlessRoleValidator serverlessRoleValidator;

        ServerlessCreateApiKeyRequestBuilder(Client client) {
            super(client);
            this.serverlessRoleValidator = new ServerlessRoleValidator();
        }

        @Override
        public CreateApiKeyRequest parse(BytesReference source, XContentType xContentType) throws IOException {
            final SourceWithXContentType sourceWithXContentType = new SourceWithXContentType(source, xContentType);
            try {
                return parseWithValidation(sourceWithXContentType);
            } catch (Exception ex) {
                logger.info("Invalid role descriptors in [API key creation request].", ex);
                throw ex;
            }
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
                serverlessRoleValidator.validateCustomRoleAndThrow(createApiKeyRequest.getRoleDescriptors(), false);
                return createApiKeyRequest;
            }
        }
    }
}
