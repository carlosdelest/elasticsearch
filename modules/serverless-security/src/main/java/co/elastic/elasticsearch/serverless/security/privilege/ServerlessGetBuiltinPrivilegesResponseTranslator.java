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

import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponseTranslator;

import java.util.TreeSet;

public class ServerlessGetBuiltinPrivilegesResponseTranslator implements GetBuiltinPrivilegesResponseTranslator {
    @Override
    public GetBuiltinPrivilegesResponse translate(GetBuiltinPrivilegesResponse response, boolean restrictResponse) {
        return new GetBuiltinPrivilegesResponse(
            new TreeSet<>(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames()),
            new TreeSet<>(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames())
        );
    }
}
