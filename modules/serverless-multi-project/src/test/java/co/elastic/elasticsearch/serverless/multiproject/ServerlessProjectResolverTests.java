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

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServerlessProjectResolverTests extends ESTestCase {

    public void testAllowAccessToAllProjectsForOperator() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            final SecurityContext context = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
            final Authentication authentication = Authentication.newRealmAuthentication(
                new User(randomAlphanumericOfLength(4)),
                new Authentication.RealmRef(randomAlphanumericOfLength(3), randomAlphanumericOfLength(3), randomAlphanumericOfLength(3))
            );
            context.executeWithAuthentication(authentication, ignore -> {
                threadPool.getThreadContext()
                    .putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
                final ServerlessProjectResolver resolver = new ServerlessProjectResolver(() -> context);
                assertThat(resolver.allowAccessToAllProjects(threadPool.getThreadContext()), is(true));

                final Set<ProjectId> projectIds = randomProjectIds();
                final ClusterState clusterState = buildClusterStateWithProjects(projectIds);
                assertThat(resolver.getProjectIds(clusterState), equalTo(projectIds));
                return null;
            });
        }
    }

    public void testAllowAccessToAllProjectsForSystemUser() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            final SecurityContext context = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
            final Authentication authentication = Authentication.newInternalAuthentication(
                InternalUsers.SYSTEM_USER,
                TransportVersion.current(),
                "node01"
            );
            context.executeWithAuthentication(authentication, ignore -> {
                final ServerlessProjectResolver resolver = new ServerlessProjectResolver(() -> context);
                assertThat(resolver.allowAccessToAllProjects(threadPool.getThreadContext()), is(true));

                final Set<ProjectId> projectIds = randomProjectIds();
                final ClusterState clusterState = buildClusterStateWithProjects(projectIds);
                assertThat(resolver.getProjectIds(clusterState), equalTo(projectIds));
                return null;
            });
        }
    }

    public void testRejectAccessToAllProjectsForNonOperator() {
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            final SecurityContext context = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
            final Authentication authentication = Authentication.newRealmAuthentication(
                new User(randomAlphanumericOfLength(4)),
                new Authentication.RealmRef(randomAlphanumericOfLength(3), randomAlphanumericOfLength(3), randomAlphanumericOfLength(3))
            );
            context.executeWithAuthentication(authentication, ignore -> {
                threadPool.getThreadContext().putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, null);
                final ServerlessProjectResolver resolver = new ServerlessProjectResolver(() -> context);
                assertThat(resolver.allowAccessToAllProjects(threadPool.getThreadContext()), is(false));

                final ClusterState clusterState = buildClusterStateWithProjects(randomProjectIds());
                expectThrows(ElasticsearchSecurityException.class, () -> resolver.getProjectIds(clusterState));
                return null;
            });

        }
    }

    private static Set<ProjectId> randomProjectIds() {
        return Set.copyOf(randomList(1, 5, ESTestCase::randomUniqueProjectId));
    }

    private static ClusterState buildClusterStateWithProjects(Set<ProjectId> projectIds) {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        projectIds.forEach(p -> metadataBuilder.put(ProjectMetadata.builder(p).build()));
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();
    }
}
