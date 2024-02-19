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

import co.elastic.elasticsearch.serverless.security.operator.ServerlessOperatorOnlyRegistry;
import co.elastic.elasticsearch.serverless.security.role.ServerlessPutRoleRequestBuilderFactory;

import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilderFactory;

module org.elasticsearch.internal.security {

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.security;

    exports co.elastic.elasticsearch.serverless.security.operator to org.elasticsearch.server;
    exports co.elastic.elasticsearch.serverless.security.role to org.elasticsearch.server;

    provides org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry with ServerlessOperatorOnlyRegistry;
    provides PutRoleRequestBuilderFactory with ServerlessPutRoleRequestBuilderFactory;
}
