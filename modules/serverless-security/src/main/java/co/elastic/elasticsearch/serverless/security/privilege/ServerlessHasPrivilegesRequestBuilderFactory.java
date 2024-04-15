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

package co.elastic.elasticsearch.serverless.security.privilege;

import co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.mustBePredefinedClusterPrivilegeMessage;
import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.mustBePredefinedIndexPrivilegeMessage;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ServerlessHasPrivilegesRequestBuilderFactory implements HasPrivilegesRequestBuilderFactory {
    private final Supplier<Boolean> strictRequestValidationEnabled;

    // Needed for java module
    public ServerlessHasPrivilegesRequestBuilderFactory() {
        this(() -> false);
    }

    // For SPI
    public ServerlessHasPrivilegesRequestBuilderFactory(ServerlessSecurityPlugin plugin) {
        this(plugin::strictHasPrivilegesRequestValidationEnabled);
    }

    private ServerlessHasPrivilegesRequestBuilderFactory(Supplier<Boolean> strictRequestValidationEnabled) {
        this.strictRequestValidationEnabled = strictRequestValidationEnabled;
    }

    @Override
    public HasPrivilegesRequestBuilder create(Client client, boolean restrictRequest) {
        return new ServerlessHasPrivilegesRequestBuilder(client, restrictRequest, strictRequestValidationEnabled);
    }

    static class ServerlessHasPrivilegesRequestBuilder extends HasPrivilegesRequestBuilder {
        private static final Logger logger = LogManager.getLogger(ServerlessHasPrivilegesRequestBuilder.class);
        private final boolean restrictRequest;
        private final Supplier<Boolean> strictRequestValidationEnabled;

        ServerlessHasPrivilegesRequestBuilder(
            ElasticsearchClient client,
            boolean restrictRequest,
            Supplier<Boolean> strictRequestValidationEnabled
        ) {
            super(client);
            this.restrictRequest = restrictRequest;
            this.strictRequestValidationEnabled = strictRequestValidationEnabled;
        }

        @Override
        public HasPrivilegesRequestBuilder source(String username, BytesReference source, XContentType xContentType) throws IOException {
            if (false == restrictRequest) {
                return super.source(username, source, xContentType);
            }
            super.source(username, source, xContentType);
            validatePrivilegesToCheck(username, request.getPrivilegesToCheck());
            return this;
        }

        private void validatePrivilegesToCheck(String username, AuthorizationEngine.PrivilegesToCheck privilegesToCheck) {
            ActionRequestValidationException validationException = null;
            for (var clusterPrivilege : privilegesToCheck.cluster()) {
                validationException = validateClusterPrivilege(clusterPrivilege, validationException);
            }
            for (var indexPrivilege : privilegesToCheck.index()) {
                for (var indexPrivilegeName : indexPrivilege.getPrivileges()) {
                    validationException = validateIndexPrivilege(indexPrivilegeName, validationException);
                }
            }
            if (validationException != null) {
                if (strictRequestValidationEnabled.get()) {
                    throw validationException;
                } else {
                    // We have some noisy issues that are already tracked; exclude them from logs
                    if (false == isKnownNoisyIssue(privilegesToCheck)) {
                        logger.info(
                            "Has Privileges Request includes unsupported privileges for ["
                                + username
                                + "] and privileges to check ["
                                + privilegesToCheck
                                + "]",
                            validationException
                        );
                    } else {
                        logger.debug("Detected known invalid Has Privileges Request for [" + username + "]");
                    }
                }
            }
        }

        private static final Set<String> KNOWN_INVALID_PAYLOAD_ES_8038 = Set.of(
            "all",
            "create_snapshot",
            "manage",
            "manage_api_key",
            "manage_ccr",
            "manage_transform",
            "manage_ilm",
            "manage_index_templates",
            "manage_ingest_pipelines",
            "manage_ml",
            "manage_own_api_key",
            "manage_pipeline",
            "manage_rollup",
            "manage_saml",
            "manage_security",
            "manage_token",
            "manage_watcher",
            "monitor",
            "monitor_transform",
            "monitor_ml",
            "monitor_rollup",
            "monitor_watcher",
            "read_ccr",
            "read_ilm",
            "transport_client"
        );

        private static boolean isKnownNoisyIssue(AuthorizationEngine.PrivilegesToCheck privilegesToCheck) {
            if (privilegesToCheck.cluster() == null) {
                return false;
            }
            // https://elasticco.atlassian.net/browse/ES-8038
            return new HashSet<>(Arrays.asList(privilegesToCheck.cluster())).equals(KNOWN_INVALID_PAYLOAD_ES_8038);
        }

        private ActionRequestValidationException validateClusterPrivilege(
            String clusterPrivilege,
            @Nullable ActionRequestValidationException validationException
        ) {
            // Raw actions and patterns are allowed, so nothing else to validate here
            if (ClusterPrivilegeResolver.isClusterAction(clusterPrivilege)) {
                return validationException;
            }
            if (ClusterPrivilegeResolver.getNamedOrNull(clusterPrivilege) == null) {
                return addValidationError(
                    "unknown cluster privilege ["
                        + clusterPrivilege
                        + "]. "
                        + mustBePredefinedClusterPrivilegeMessage()
                        + " or a pattern over one of the available cluster actions",
                    validationException
                );
            } else if (false == ServerlessSupportedPrivilegesRegistry.isSupportedClusterPrivilege(clusterPrivilege)) {
                return addValidationError(
                    "cluster privilege ["
                        + clusterPrivilege
                        + "] exists but is not supported when running in serverless mode. "
                        + mustBePredefinedClusterPrivilegeMessage()
                        + " or a pattern over one of the available cluster actions",
                    validationException
                );
            }
            return validationException;
        }

        private ActionRequestValidationException validateIndexPrivilege(
            String indexPrivilegeName,
            @Nullable ActionRequestValidationException validationException
        ) {
            // Raw actions and patterns are allowed, so nothing else to validate here
            if (IndexPrivilege.ACTION_MATCHER.test(indexPrivilegeName)) {
                return validationException;
            }
            if (IndexPrivilege.getNamedOrNull(indexPrivilegeName) == null) {
                return addValidationError(
                    "unknown index privilege ["
                        + indexPrivilegeName
                        + "]. "
                        + mustBePredefinedIndexPrivilegeMessage()
                        + " or a pattern over one of the available index actions",
                    validationException
                );
            } else if (false == ServerlessSupportedPrivilegesRegistry.isSupportedIndexPrivilege(indexPrivilegeName)) {
                return addValidationError(
                    "index privilege ["
                        + indexPrivilegeName
                        + "] exists but is not supported when running in serverless mode. "
                        + mustBePredefinedIndexPrivilegeMessage()
                        + " or a pattern over one of the available index actions",
                    validationException
                );
            }
            return validationException;
        }
    }
}
