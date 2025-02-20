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

package co.elastic.elasticsearch.serverless.multiproject;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.operator.OperatorPrivilegesUtil;

import java.util.function.Supplier;

public class ServerlessProjectResolver extends AbstractProjectResolver {

    public ServerlessProjectResolver(Supplier<ThreadContext> threadContext) {
        super(threadContext);
    }

    @Override
    protected ProjectId getFallbackProjectId() {
        throw new IllegalStateException("No project id found in thread context");
    }

    @Override
    protected boolean allowAccessToAllProjects(ThreadContext threadContext) {
        return OperatorPrivilegesUtil.isOperator(threadContext);
    }
}
