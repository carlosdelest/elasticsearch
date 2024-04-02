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

import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;

import java.util.function.Predicate;

public class ServerlessReservedRoleNameChecker extends ReservedRoleNameChecker.Default {

    private final Predicate<String> fileRoleStoreNameChecker;

    public ServerlessReservedRoleNameChecker(Predicate<String> fileRoleStoreNameChecker) {
        this.fileRoleStoreNameChecker = fileRoleStoreNameChecker;
    }

    @Override
    public boolean isReserved(String roleName) {
        return super.isReserved(roleName) || fileRoleStoreNameChecker.test(roleName);
    }

    public static class Factory implements ReservedRoleNameChecker.Factory {
        @Override
        public ReservedRoleNameChecker create(Predicate<String> fileRoleStoreNameChecker) {
            return new ServerlessReservedRoleNameChecker(fileRoleStoreNameChecker);
        }
    }
}
