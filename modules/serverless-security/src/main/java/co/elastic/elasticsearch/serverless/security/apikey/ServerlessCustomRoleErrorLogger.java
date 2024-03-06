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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;

final class ServerlessCustomRoleErrorLogger {
    private static final Logger LOGGER = LogManager.getLogger(ServerlessCustomRoleErrorLogger.class);

    private ServerlessCustomRoleErrorLogger() {}

    static <T> void logCustomRoleErrors(
        String payloadDescription,
        T payload,
        CheckedConsumer<T, IOException> payloadChecker,
        @Nullable List<RoleDescriptor> roleDescriptors
    ) {
        logCustomRoleErrors(payloadDescription, payload, payloadChecker, roleDescriptors, LOGGER);
    }

    /**
     * This method accepts a generic payload and payload checker. If the payload checker throws, the exception is logged.
     *
     * This logic is only executed if the passed in role descriptors are all valid, regular role descriptors.
     *
     * This allows us to log custom role validation errors without logging generic role descriptor validation errors.
     *
     * For example usage see {@link ServerlessBulkUpdateApiKeyRequestTranslator#translate(RestRequest)}.
     */
    static <T> void logCustomRoleErrors(
        String payloadDescription,
        T payloadWithCustomRoleDescriptors,
        CheckedConsumer<T, IOException> payloadValidator,
        @Nullable List<RoleDescriptor> roleDescriptors,
        Logger logger
    ) {
        if (roleDescriptors == null || roleDescriptors.isEmpty()) {
            return;
        }
        ActionRequestValidationException validationException = null;
        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            validationException = RoleDescriptorRequestValidator.validate(roleDescriptor, validationException);
        }
        if (validationException == null) {
            try {
                payloadValidator.accept(payloadWithCustomRoleDescriptors);
            } catch (Exception ex) {
                logger.info("Invalid custom role descriptors in [" + payloadDescription + "].", ex);
            }
        } else {
            logger.debug(
                "Custom role validation detected generic role validation errors for [" + payloadDescription + "]",
                validationException
            );
        }
    }
}
