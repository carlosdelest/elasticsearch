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

package co.elastic.elasticsearch.serverless.security.authc;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.cluster.project.TestProjectResolvers.usingRequestHeader;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectServiceAccountTokenStoreTests extends ESTestCase {

    private ProjectResolver projectResolver;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private static Hasher hasher;

    @Before
    public void init() {
        hasher = getFastStoredHashAlgoForTests();
        ThreadPool threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        projectResolver = usingRequestHeader(threadContext);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(ProjectFileSettingsRealmSettings.getSettings())
        );
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    public void testAuthenticate() {
        var tokenStore = new ProjectServiceAccountTokenStore(clusterService, projectResolver);
        var serviceAccount = randomFrom(ServiceAccountService.getServiceAccounts().values());

        for (var projectId : randomList(3, 5, ESTestCase::randomUniqueProjectId)) {
            setActiveProjectId(projectId);
            var successfulTokenName = randomAlphaOfLengthBetween(5, 20);
            var testToken = ServiceAccountToken.newToken(serviceAccount.id(), successfulTokenName);
            addTokenHashToClusterState(projectId, testToken);
            {
                PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
                tokenStore.authenticate(testToken, future);
                var result = future.actionGet();
                assertTrue(result.isSuccess());
                assertThat(result.getTokenSource(), is(TokenInfo.TokenSource.FILE));
            }
            {
                PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
                tokenStore.authenticate(
                    ServiceAccountToken.newToken(
                        serviceAccount.id(),
                        randomValueOtherThan(successfulTokenName, () -> randomAlphaOfLengthBetween(5, 20))
                    ),
                    future
                );
                var result = future.actionGet();
                assertFalse(result.isSuccess());
                assertThat(result.getTokenSource(), is(TokenInfo.TokenSource.FILE));
            }
        }
    }

    public void testAuthenticationCacheUsed() {
        var tokenStore = new ProjectServiceAccountTokenStore(clusterService, projectResolver);
        var serviceAccount = randomFrom(ServiceAccountService.getServiceAccounts().values());

        long keysInCache = 1;
        for (var projectId : randomList(3, 5, ESTestCase::randomUniqueProjectId)) {
            setActiveProjectId(projectId);
            var successfulTokenName = randomAlphaOfLengthBetween(5, 20);
            var testToken = ServiceAccountToken.newToken(serviceAccount.id(), successfulTokenName);
            addTokenHashToClusterState(projectId, testToken);
            {
                PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
                tokenStore.authenticate(testToken, future);
                var result = future.actionGet();
                assertTrue(result.isSuccess());
                assertThat(result.getTokenSource(), is(TokenInfo.TokenSource.FILE));
            }
            {
                PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
                tokenStore.authenticate(testToken, future);
                var result = future.actionGet();
                assertTrue(result.isSuccess());
                assertThat(result.getTokenSource(), is(TokenInfo.TokenSource.FILE));
            }

            assertThat(tokenStore.cacheStats().getEvictions(), equalTo(0L));
            assertThat(tokenStore.cacheStats().getHits(), equalTo(keysInCache));
            assertThat(tokenStore.cacheStats().getMisses(), equalTo(keysInCache));
            keysInCache++;
        }
    }

    public void testChangeTokenHash() {
        var tokenStore = new ProjectServiceAccountTokenStore(clusterService, projectResolver);
        var serviceAccount = randomFrom(ServiceAccountService.getServiceAccounts().values());
        ProjectId projectId = randomUniqueProjectId();
        var tokenName = randomAlphaOfLengthBetween(5, 20);
        var testToken = ServiceAccountToken.newToken(serviceAccount.id(), tokenName);
        setActiveProjectId(projectId);
        addTokenHashToClusterState(projectId, testToken);

        {
            PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
            tokenStore.authenticate(testToken, future);
            var result = future.actionGet();
            assertTrue(result.isSuccess());
        }

        var newToken = ServiceAccountToken.newToken(serviceAccount.id(), tokenName);
        addTokenHashToClusterState(projectId, newToken);

        for (var tokenToTest : shuffledList(List.of(newToken, testToken))) {
            PlainActionFuture<ServiceAccountTokenStore.StoreAuthenticationResult> future = new PlainActionFuture<>();
            tokenStore.authenticate(tokenToTest, future);
            var result = future.actionGet();
            assertEquals(result.isSuccess(), tokenToTest.equals(newToken));
        }
    }

    private void addTokenHashToClusterState(ProjectId projectId, ServiceAccountToken token) {
        MockSecureSettings settings = new MockSecureSettings();
        var hashSetting = ProjectServiceAccountTokenStoreSettings.SERVICE_TOKEN_HASH_SETTING_BY_ACCOUNT_ID.get(token.getAccountId())
            .getConcreteSettingForNamespace(token.getTokenName());
        settings.setFile(hashSetting.getKey(), serviceAccountTokenSecretHash(token).getBytes(StandardCharsets.UTF_8));
        ClusterState clusterStateWithSecrets = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(ProjectSecrets.TYPE, new ProjectSecrets(new SecureClusterStateSettings(settings)))
                    .build()
            )
            .build();

        when(clusterService.state()).thenReturn(clusterStateWithSecrets);
    }

    private void setActiveProjectId(ProjectId projectId) {
        threadContext.newEmptyContext();
        threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id());
    }

    private static Hasher getFastStoredHashAlgoForTests() {
        return inFipsJvm()
            ? Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch"))
            : Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch", "bcrypt", "bcrypt9"));
    }

    private static String serviceAccountTokenSecretHash(ServiceAccountToken serviceAccountToken) {
        return new String(hasher.hash(new SecureString(serviceAccountToken.getSecret().toString().toCharArray())));
    }
}
