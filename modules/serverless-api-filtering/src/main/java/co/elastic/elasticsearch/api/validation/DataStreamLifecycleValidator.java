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

package co.elastic.elasticsearch.api.validation;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

/**
 * A class to perform validation on the lifecycle parameters for serverless.
 * Currently, it rejects requests that define the <code>enabled</code> flag as false when coming from a non operator user.
 */
public class DataStreamLifecycleValidator {

    private final ThreadContext threadContext;

    public DataStreamLifecycleValidator(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Validates if a public user (no operator privileges) is trying to disable the lifecycle of a data stream or a template.
     * It does not perform this validation if operator privileges are set.
     *
     * @param lifecycle - lifecycle from the request
     * @throws IllegalArgumentException with a message indicating that the `enabled=false` needs to be removed.
     */
    public void validateLifecycle(@Nullable DataStreamLifecycle lifecycle) {
        if (isOperator() == false) {
            if (lifecycle != null && lifecycle.isEnabled() == false) {
                throw new IllegalArgumentException("Data stream lifecycle cannot be disabled in serverless, please remove 'enabled=false'");
            }
        }

    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }

}
