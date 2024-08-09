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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequestTranslator;

import java.io.IOException;

public class ServerlessBulkUpdateApiKeyRequestTranslator extends BulkUpdateApiKeyRequestTranslator.Default {
    private static final Logger logger = LogManager.getLogger(ServerlessBulkUpdateApiKeyRequestTranslator.class);
    private static final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> PARSER = createParser(
        ServerlessCustomRoleParser::parseApiKeyRoleDescriptor
    );
    private final ServerlessRoleValidator serverlessRoleValidator;

    public ServerlessBulkUpdateApiKeyRequestTranslator() {
        this.serverlessRoleValidator = new ServerlessRoleValidator();
    }

    @Override
    public BulkUpdateApiKeyRequest translate(RestRequest request) throws IOException {
        try {
            return parseWithValidation(request);
        } catch (Exception ex) {
            logger.info("Invalid role descriptors in [API key bulk update request].", ex);
            throw ex;
        }
    }

    private BulkUpdateApiKeyRequest parseWithValidation(RestRequest request) throws IOException {
        final BulkUpdateApiKeyRequest updateApiKeyRequest = PARSER.parse(request.contentParser(), null);
        serverlessRoleValidator.validateCustomRoleAndThrow(updateApiKeyRequest.getRoleDescriptors(), false);
        return updateApiKeyRequest;
    }

}
