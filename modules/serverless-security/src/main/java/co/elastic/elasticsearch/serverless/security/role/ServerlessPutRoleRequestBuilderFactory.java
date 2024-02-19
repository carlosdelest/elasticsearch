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

package co.elastic.elasticsearch.serverless.security.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.function.Predicate;

public class ServerlessPutRoleRequestBuilderFactory implements PutRoleRequestBuilderFactory {
    @Override
    public PutRoleRequestBuilder create(Client client, boolean restrictRequest, Predicate<String> fileRolesStoreNameChecker) {
        return new ServerlessPutRoleRequestBuilder(client, restrictRequest, fileRolesStoreNameChecker);
    }

    static class ServerlessPutRoleRequestBuilder extends PutRoleRequestBuilder {
        private final boolean restrictRequest;
        private final ServerlessCustomRoleValidator serverlessCustomRoleValidator;

        ServerlessPutRoleRequestBuilder(Client client, boolean restrictRequest, Predicate<String> fileRolesStoreNameChecker) {
            super(client);
            this.restrictRequest = restrictRequest;
            this.serverlessCustomRoleValidator = new ServerlessCustomRoleValidator(fileRolesStoreNameChecker);
        }

        @Override
        public PutRoleRequestBuilder source(String name, BytesReference source, XContentType xContentType) throws IOException {
            if (false == restrictRequest) {
                return super.source(name, source, xContentType);
            }
            final RoleDescriptor roleDescriptor = ServerlessCustomRoleParser.parse(name, source, xContentType);
            assert name.equals(roleDescriptor.getName());
            assert false == roleDescriptor.hasConfigurableClusterPrivileges();
            assert false == roleDescriptor.hasRemoteIndicesPrivileges();
            assert false == roleDescriptor.hasWorkflowsRestriction();
            assert false == roleDescriptor.hasRunAs();
            request.name(name);
            request.cluster(roleDescriptor.getClusterPrivileges());
            request.addIndex(roleDescriptor.getIndicesPrivileges());
            request.addApplicationPrivileges(roleDescriptor.getApplicationPrivileges());
            request.metadata(roleDescriptor.getMetadata());
            validate(request);
            return this;
        }

        private void validate(PutRoleRequest request) {
            final ActionRequestValidationException validationException = serverlessCustomRoleValidator.validate(request.roleDescriptor());
            if (validationException != null) {
                throw validationException;
            }
        }
    }
}
