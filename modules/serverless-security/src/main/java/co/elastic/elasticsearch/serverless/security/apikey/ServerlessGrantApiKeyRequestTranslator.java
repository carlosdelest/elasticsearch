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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction;

import java.io.IOException;

public class ServerlessGrantApiKeyRequestTranslator extends RestGrantApiKeyAction.RequestTranslator.Default {
    private static final Logger logger = LogManager.getLogger(ServerlessGrantApiKeyRequestTranslator.class);

    private static final ObjectParser<GrantApiKeyRequest, Void> PARSER = createParser(
        ServerlessCustomRoleParser::parseWithWorkflowRestrictionAllowed
    );

    private final ServerlessRoleValidator serverlessRoleValidator;

    public ServerlessGrantApiKeyRequestTranslator() {
        this.serverlessRoleValidator = new ServerlessRoleValidator();
    }

    @Override
    public GrantApiKeyRequest translate(RestRequest request) throws IOException {
        try {
            return parseWithValidation(request);
        } catch (Exception ex) {
            logger.info("Invalid role descriptors in [Grant API key request].", ex);
            return super.translate(request);
        }
    }

    private GrantApiKeyRequest parseWithValidation(RestRequest request) throws IOException {
        try (XContentParser xContentParser = request.contentParser()) {
            final GrantApiKeyRequest grantApiKeyRequest = PARSER.parse(xContentParser, null);
            serverlessRoleValidator.validateCustomRoleAndThrow(grantApiKeyRequest.getApiKeyRequest().getRoleDescriptors(), false);
            return grantApiKeyRequest;
        }
    }
}
