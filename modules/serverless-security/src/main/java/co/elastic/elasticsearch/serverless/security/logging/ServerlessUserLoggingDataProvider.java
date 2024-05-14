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

package co.elastic.elasticsearch.serverless.security.logging;

import co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin;

import org.elasticsearch.plugins.internal.LoggingDataProvider;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;
import java.util.function.Supplier;

public class ServerlessUserLoggingDataProvider implements LoggingDataProvider {

    private final Supplier<SecurityContext> securityContext;

    public ServerlessUserLoggingDataProvider() {
        // Needed for SPI, but never used
        throw new UnsupportedOperationException("Provider must be constructed with a plugin reference");
    }

    public ServerlessUserLoggingDataProvider(ServerlessSecurityPlugin plugin) {
        this(plugin::getSecurityContext);
    }

    // For testing
    ServerlessUserLoggingDataProvider(Supplier<SecurityContext> securityContext) {
        this.securityContext = securityContext;
    }

    @Override
    public void collectData(Map<String, String> data) {
        final Authentication authentication = getAuthentication();
        if (authentication != null) {
            final Subject subject = authentication.getEffectiveSubject();
            final User user = subject.getUser();
            data.put("user.name", user.principal());
            if (user.fullName() != null) {
                data.put("user.full_name", user.fullName());
            }
            if (authentication.isApiKey()) {
                data.put("apikey.id", String.valueOf(subject.getMetadata().get(AuthenticationField.API_KEY_ID_KEY)));
            } else {
                data.put("user.realm", subject.getRealm().getName());
            }
            data.put("auth.type", authentication.getAuthenticationType().name());
        }
    }

    private Authentication getAuthentication() {
        return securityContext.get() != null ? securityContext.get().getAuthentication() : null;
    }

}
