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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ServerlessHasPrivilegesRequestBuilderFactory implements HasPrivilegesRequestBuilderFactory {

    // Needed for java module
    public ServerlessHasPrivilegesRequestBuilderFactory() {}

    @Override
    public HasPrivilegesRequestBuilder create(Client client, boolean restrictRequest) {
        return new ServerlessHasPrivilegesRequestBuilder(client);
    }

    static class ServerlessHasPrivilegesRequestBuilder extends HasPrivilegesRequestBuilder {
        private static final Logger logger = LogManager.getLogger(ServerlessHasPrivilegesRequestBuilder.class);

        ServerlessHasPrivilegesRequestBuilder(ElasticsearchClient client) {
            super(client);
        }

        @Override
        public HasPrivilegesRequestBuilder source(String username, BytesReference source, XContentType xContentType) throws IOException {
            super.source(username, source, xContentType);
            addHeaderWarningForUnsupportedPrivileges(username, request.getPrivilegesToCheck());
            return this;
        }

        private void addHeaderWarningForUnsupportedPrivileges(String username, AuthorizationEngine.PrivilegesToCheck privilegesToCheck) {
            final SortedSet<String> unsupportedClusterPrivileges = Arrays.stream(privilegesToCheck.cluster())
                .filter(this::isUnsupportedClusterPrivilege)
                .collect(Collectors.toCollection(TreeSet::new));
            final SortedSet<String> unsupportedIndexPrivileges = Arrays.stream(privilegesToCheck.index())
                .flatMap(indexPrivilege -> Arrays.stream(indexPrivilege.getPrivileges()))
                .filter(this::isUnsupportedIndexPrivilege)
                .collect(Collectors.toCollection(TreeSet::new));

            String headerWarningMessage = "";
            if (false == unsupportedClusterPrivileges.isEmpty()) {
                headerWarningMessage += " cluster privileges: ["
                    + Strings.collectionToCommaDelimitedString(unsupportedClusterPrivileges)
                    + "];";
            }
            if (false == unsupportedIndexPrivileges.isEmpty()) {
                headerWarningMessage += " index privileges: ["
                    + Strings.collectionToCommaDelimitedString(unsupportedIndexPrivileges)
                    + "];";
            }
            if (false == headerWarningMessage.isEmpty()) {
                logger.info(
                    "HasPrivileges request has unsupported privileges for ["
                        + username
                        + "] and privileges to check ["
                        + privilegesToCheck
                        + "]; "
                        + headerWarningMessage
                );
                HeaderWarning.addWarning(
                    "HasPrivileges request includes privileges for features not available in serverless mode; "
                        + "you will not have access to these features regardless of your permissions;"
                        + headerWarningMessage
                );
            }
        }

        private boolean isUnsupportedClusterPrivilege(String clusterPrivilege) {
            // Raw actions and patterns are allowed, so skip
            if (ClusterPrivilegeResolver.isClusterAction(clusterPrivilege)) {
                return false;
            }
            // Any bogus, named privilege will be caught later; we don't care about these here, so skip
            if (ClusterPrivilegeResolver.getNamedOrNull(clusterPrivilege) == null) {
                return false;
            }
            return false == ServerlessSupportedPrivilegesRegistry.isSupportedClusterPrivilege(clusterPrivilege);
        }

        private boolean isUnsupportedIndexPrivilege(String indexPrivilegeName) {
            // Raw actions and patterns are allowed, so skip
            if (IndexPrivilege.ACTION_MATCHER.test(indexPrivilegeName)) {
                return false;
            }
            // Any bogus, named privilege will be caught later; we don't care about these here, so skip
            if (IndexPrivilege.getNamedOrNull(indexPrivilegeName) == null) {
                return false;
            }
            return false == ServerlessSupportedPrivilegesRegistry.isSupportedIndexPrivilege(indexPrivilegeName);
        }
    }
}
