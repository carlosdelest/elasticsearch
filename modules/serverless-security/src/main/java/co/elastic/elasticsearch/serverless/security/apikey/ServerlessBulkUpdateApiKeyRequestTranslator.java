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
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequestTranslator;

import java.io.IOException;
import java.util.function.Supplier;

public class ServerlessBulkUpdateApiKeyRequestTranslator extends BulkUpdateApiKeyRequestTranslator.Default {
    private static final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> PARSER = createParser(
        ServerlessCustomRoleParser::parseApiKeyRoleDescriptor
    );
    private final ServerlessRoleValidator serverlessRoleValidator;
    private final Supplier<Boolean> strictRequestValidationEnabled;
    private final Supplier<Boolean> operatorStrictRoleValidationEnabled;

    // Needed for java module
    public ServerlessBulkUpdateApiKeyRequestTranslator() {
        this(new ServerlessRoleValidator(), () -> false, () -> false);
    }

    // For SPI
    public ServerlessBulkUpdateApiKeyRequestTranslator(ServerlessSecurityPlugin plugin) {
        this(new ServerlessRoleValidator(), plugin::strictApiKeyRequestValidationEnabled, plugin::isOperatorStrictRoleValidationEnabled);
    }

    ServerlessBulkUpdateApiKeyRequestTranslator(
        ServerlessRoleValidator serverlessRoleValidator,
        Supplier<Boolean> strictRequestValidationEnabled,
        Supplier<Boolean> operatorStrictRoleValidationEnabled
    ) {
        this.serverlessRoleValidator = serverlessRoleValidator;
        this.strictRequestValidationEnabled = strictRequestValidationEnabled;
        this.operatorStrictRoleValidationEnabled = operatorStrictRoleValidationEnabled;
    }

    @Override
    public BulkUpdateApiKeyRequest translate(RestRequest request) throws IOException {
        if (shouldApplyStrictValidation(request)) {
            try {
                return parseWithValidation(request);
            } catch (Exception ex) {
                ServerlessCustomRoleErrorLogger.logException("API key bulk update request", ex);
                throw ex;
            }
        }

        final BulkUpdateApiKeyRequest updateRequest = super.translate(request);
        ServerlessCustomRoleErrorLogger.logCustomRoleErrors(
            "API key bulk update request",
            request,
            this::parseWithValidation,
            updateRequest.getRoleDescriptors()
        );

        return updateRequest;
    }

    private boolean shouldApplyStrictValidation(RestRequest request) {
        final boolean restrictRequest = request.hasParam(RestRequest.PATH_RESTRICTED);
        if (restrictRequest) {
            return strictRequestValidationEnabled.get();
        } else {
            return operatorStrictRoleValidationEnabled.get();
        }
    }

    private BulkUpdateApiKeyRequest parseWithValidation(RestRequest request) throws IOException {
        final BulkUpdateApiKeyRequest updateApiKeyRequest = PARSER.parse(request.contentParser(), null);
        serverlessRoleValidator.validateCustomRoleAndThrow(updateApiKeyRequest.getRoleDescriptors(), false);
        return updateApiKeyRequest;
    }
}
