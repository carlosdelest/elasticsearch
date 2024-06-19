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
import co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequestTranslator;

import java.io.IOException;
import java.util.function.Supplier;

public class ServerlessUpdateApiKeyRequestTranslator extends UpdateApiKeyRequestTranslator.Default {
    private static final ConstructingObjectParser<Payload, Void> PARSER = createParser(
        ServerlessCustomRoleParser::parseApiKeyRoleDescriptor
    );
    private final ServerlessRoleValidator serverlessRoleValidator;
    private final Supplier<Boolean> operatorStrictRoleValidationEnabled;

    // Needed for java module
    public ServerlessUpdateApiKeyRequestTranslator() {
        this(new ServerlessRoleValidator(), () -> false);
    }

    // For SPI
    public ServerlessUpdateApiKeyRequestTranslator(ServerlessSecurityPlugin plugin) {
        this(new ServerlessRoleValidator(), plugin::isOperatorStrictRoleValidationEnabled);
    }

    ServerlessUpdateApiKeyRequestTranslator(
        ServerlessRoleValidator serverlessRoleValidator,
        Supplier<Boolean> operatorStrictRoleValidationEnabled
    ) {
        this.serverlessRoleValidator = serverlessRoleValidator;
        this.operatorStrictRoleValidationEnabled = operatorStrictRoleValidationEnabled;
    }

    @Override
    public UpdateApiKeyRequest translate(RestRequest request) throws IOException {
        // Note that we use `ids` here even though we only support a single ID. This is because the route where this translator is used
        // shares a path prefix with `RestClearApiKeyCacheAction` and our current REST implementation requires that path params have the
        // same wildcard if their paths share a prefix
        final String apiKeyId = request.param("ids");
        if (false == request.hasContent()) {
            return UpdateApiKeyRequest.usingApiKeyId(apiKeyId);
        }

        final RequestWithApiKeyId requestWithApiKeyId = new RequestWithApiKeyId(apiKeyId, request);
        if (shouldApplyStrictValidation(request)) {
            try {
                return parseWithValidation(requestWithApiKeyId);
            } catch (Exception ex) {
                ServerlessCustomRoleErrorLogger.logException(
                    "API key update request for API key [" + requestWithApiKeyId.apiKeyId() + "]",
                    ex
                );
                throw ex;
            }
        }

        final UpdateApiKeyRequest updateRequest = super.translate(request);
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "API key update request for API key [" + requestWithApiKeyId.apiKeyId() + "]",
            requestWithApiKeyId,
            this::parseWithValidation,
            updateRequest.getRoleDescriptors()
        );

        return updateRequest;
    }

    private boolean shouldApplyStrictValidation(RestRequest request) {
        final boolean restrictRequest = request.hasParam(RestRequest.PATH_RESTRICTED);
        if (restrictRequest) {
            return true;
        } else {
            return operatorStrictRoleValidationEnabled.get();
        }
    }

    private UpdateApiKeyRequest parseWithValidation(RequestWithApiKeyId requestWithApiKeyId) throws IOException {
        final UpdateApiKeyRequest updateApiKeyRequest = parse(requestWithApiKeyId);
        serverlessRoleValidator.validateCustomRoleAndThrow(updateApiKeyRequest.getRoleDescriptors(), false);
        return updateApiKeyRequest;
    }

    private UpdateApiKeyRequest parse(RequestWithApiKeyId requestWithApiKeyId) throws IOException {
        assert requestWithApiKeyId.request().hasContent();
        try (XContentParser parser = requestWithApiKeyId.request().contentParser()) {
            final Payload payload = PARSER.parse(parser, null);
            return new UpdateApiKeyRequest(
                requestWithApiKeyId.apiKeyId(),
                payload.roleDescriptors(),
                payload.metadata(),
                payload.expiration()
            );
        }
    }

    record RequestWithApiKeyId(String apiKeyId, RestRequest request) {}
}
