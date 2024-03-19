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

package co.elastic.elasticsearch.serverless.security.authz;

import co.elastic.elasticsearch.serverless.security.privilege.ServerlessSupportedPrivilegesRegistry;

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authz.AuthorizationDenialMessages;

import java.util.Collection;

public class ServerlessAuthorizationDenialMessages extends AuthorizationDenialMessages.Default {
    @Override
    protected Collection<String> findClusterPrivilegesThatGrant(Authentication authentication, String action, TransportRequest request) {
        return ServerlessSupportedPrivilegesRegistry.findClusterPrivilegesThatGrant(authentication, action, request);
    }

    @Override
    protected Collection<String> findIndexPrivilegesThatGrant(String action) {
        return ServerlessSupportedPrivilegesRegistry.findIndexPrivilegesThatGrant(action);
    }
}
