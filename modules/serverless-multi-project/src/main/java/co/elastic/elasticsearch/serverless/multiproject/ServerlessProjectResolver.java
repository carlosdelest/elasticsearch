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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.operator.OperatorPrivilegesUtil;
import org.elasticsearch.xpack.core.security.user.InternalUsers;

import java.util.function.Supplier;

public class ServerlessProjectResolver extends AbstractProjectResolver {

    private final Logger logger = LogManager.getLogger(ServerlessProjectResolver.class);
    private final Supplier<SecurityContext> securityContextSupplier;

    public ServerlessProjectResolver(Supplier<SecurityContext> securityContextSupplier) {
        super(() -> securityContextSupplier.get().getThreadContext());
        this.securityContextSupplier = securityContextSupplier;
    }

    @Override
    @FixForMultiProject(description = "This should throw an exception")
    protected ProjectId getFallbackProjectId() {
        return Metadata.DEFAULT_PROJECT_ID;
    }

    @Override
    protected boolean allowAccessToAllProjects(ThreadContext threadContext) {
        if (OperatorPrivilegesUtil.isOperator(threadContext)) {
            return true;
        }
        final SecurityContext securityContext = securityContextSupplier.get();
        final Authentication auth = securityContext.getAuthentication();
        if (auth == null) {
            // This should never be possible, but is, and we need to fix it
            @FixForMultiProject(description = "Should require a user")
            final var allowAllProjectsWhenThereIsNoUser = true;
            return allowAllProjectsWhenThereIsNoUser;
        }
        if (InternalUsers.SYSTEM_USER.equals(auth.getEffectiveSubject().getUser())) {
            return true;
        }
        logger.debug("User [{}] is not an operator", auth);
        return false;
    }
}
