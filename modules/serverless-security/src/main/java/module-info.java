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

import co.elastic.elasticsearch.serverless.security.apikey.ServerlessBulkUpdateApiKeyRequestTranslator;
import co.elastic.elasticsearch.serverless.security.apikey.ServerlessCreateApiKeyRequestBuilderFactory;
import co.elastic.elasticsearch.serverless.security.apikey.ServerlessUpdateApiKeyRequestTranslator;
import co.elastic.elasticsearch.serverless.security.authz.ServerlessAuthorizationDenialMessages;
import co.elastic.elasticsearch.serverless.security.operator.ServerlessOperatorOnlyRegistry;
import co.elastic.elasticsearch.serverless.security.privilege.ServerlessGetBuiltinPrivilegesResponseTranslator;
import co.elastic.elasticsearch.serverless.security.privilege.ServerlessHasPrivilegesRequestBuilderFactory;
import co.elastic.elasticsearch.serverless.security.role.ServerlessPutRoleRequestBuilderFactory;
import co.elastic.elasticsearch.serverless.security.role.ServerlessReservedRoleNameChecker;

import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequestTranslator;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequestTranslator;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponseTranslator;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilderFactory;
import org.elasticsearch.xpack.security.authz.AuthorizationDenialMessages;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;

module org.elasticsearch.internal.security {

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.security;

    exports co.elastic.elasticsearch.serverless.security.apikey to org.elasticsearch.server;
    exports co.elastic.elasticsearch.serverless.security.operator to org.elasticsearch.server;
    exports co.elastic.elasticsearch.serverless.security.role to org.elasticsearch.server;
    exports co.elastic.elasticsearch.serverless.security.privilege to org.elasticsearch.server;
    exports co.elastic.elasticsearch.serverless.security.authz to org.elasticsearch.server;

    provides UpdateApiKeyRequestTranslator with ServerlessUpdateApiKeyRequestTranslator;
    provides BulkUpdateApiKeyRequestTranslator with ServerlessBulkUpdateApiKeyRequestTranslator;
    provides org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry with ServerlessOperatorOnlyRegistry;
    provides PutRoleRequestBuilderFactory with ServerlessPutRoleRequestBuilderFactory;
    provides CreateApiKeyRequestBuilderFactory with ServerlessCreateApiKeyRequestBuilderFactory;
    provides GetBuiltinPrivilegesResponseTranslator with ServerlessGetBuiltinPrivilegesResponseTranslator;
    provides HasPrivilegesRequestBuilderFactory with ServerlessHasPrivilegesRequestBuilderFactory;
    provides AuthorizationDenialMessages with ServerlessAuthorizationDenialMessages;
    provides ReservedRoleNameChecker.Factory with ServerlessReservedRoleNameChecker.Factory;
}
