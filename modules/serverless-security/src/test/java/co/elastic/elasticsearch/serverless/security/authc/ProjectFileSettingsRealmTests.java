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
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static co.elastic.elasticsearch.serverless.security.authc.ProjectFileSettingsRealmSettings.PROJECT_FILE_SETTINGS_PASSWORD_HASH;
import static org.elasticsearch.cluster.project.TestProjectResolvers.usingRequestHeader;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectFileSettingsRealmTests extends ESTestCase {

    private static final String TEST_REALM_NAME = "multi-project-test";
    private static final RealmConfig.RealmIdentifier REALM_IDENTIFIER = new RealmConfig.RealmIdentifier(
        ProjectFileSettingsRealmSettings.TYPE,
        TEST_REALM_NAME
    );

    private RealmConfig realmConfig;
    private ProjectResolver projectResolver;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private static Hasher hasher;

    @Before
    public void init() {
        hasher = getFastStoredHashAlgoForTests();
        Settings globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.password_hashing.algorithm", hasher.name())
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "order", 0)
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "admin.roles", "role1,role2")
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "testing-internal.roles", "role1,role2")
            .build();

        realmConfig = getRealmConfig(globalSettings);
        ThreadPool threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        threadContext = new ThreadContext(globalSettings);
        projectResolver = usingRequestHeader(threadContext);
        ClusterSettings clusterSettings = new ClusterSettings(
            globalSettings,
            new HashSet<>(ProjectFileSettingsRealmSettings.getSettings())
        );
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(clusterService.getSettings()).thenReturn(globalSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    public void testAuthenticate() {
        String username = randomFrom("admin", "testing-internal");
        String password = randomAlphaOfLengthBetween(5, 20);
        Realm realm = createProjectFileSettingsRealm();

        for (var projectId : randomList(3, 5, ESTestCase::randomUniqueProjectId)) {
            addPasswordHashToClusterState(projectId, username, projectId.id() + password);
            setActiveProjectId(projectId);
            {
                PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
                realm.authenticate(
                    new UsernamePasswordToken(username, new SecureString((projectId.id() + password).toCharArray())),
                    future
                );
                final AuthenticationResult<User> result = future.actionGet();

                assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
                User user = result.getValue();
                assertThat(user, notNullValue());
                assertThat(user.principal(), equalTo(username));
                assertThat(user.roles(), notNullValue());
                assertThat(user.roles().length, equalTo(2));
                assertThat(user.roles(), arrayContaining("role1", "role2"));
            }
            {
                PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
                realm.authenticate(new UsernamePasswordToken(username, new SecureString("bad".toCharArray())), future);
                final AuthenticationResult<User> result = future.actionGet();
                assertThat(result.getStatus(), is(AuthenticationResult.Status.CONTINUE));
                assertThat(result.getValue(), nullValue());
            }
        }
    }

    public void testAuthenticationCacheUsed() throws ExecutionException, InterruptedException {
        String username = randomFrom("admin", "testing-internal");
        String password = randomAlphaOfLengthBetween(5, 20);
        ProjectFileSettingsRealm realm = createProjectFileSettingsRealm();

        long keysInCache = 1;
        for (var projectId : randomList(3, 5, ESTestCase::randomUniqueProjectId)) {
            addPasswordHashToClusterState(projectId, username, projectId.id() + password);
            setActiveProjectId(projectId);
            {
                final PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
                realm.authenticate(
                    new UsernamePasswordToken(username, new SecureString((projectId.id() + password).toCharArray())),
                    authFuture
                );

                var result = authFuture.get();
                assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            }
            {
                final PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
                realm.authenticate(
                    new UsernamePasswordToken(username, new SecureString((projectId.id() + password).toCharArray())),
                    authFuture
                );
                var result = authFuture.get();
                assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            }

            PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
            realm.usageStats(future);
            @SuppressWarnings("unchecked")
            Map<String, Object> stats = (Map<String, Object>) future.get().get("cache");
            assertThat(stats.get("evictions"), equalTo(0L));
            assertThat(stats.get("size"), equalTo(Long.valueOf(keysInCache).intValue()));
            assertThat(stats.get("hits"), equalTo(keysInCache));
            assertThat(stats.get("misses"), equalTo(keysInCache));
            keysInCache++;
        }
    }

    public void testChangePassword() throws ExecutionException, InterruptedException {
        String username = randomFrom("admin", "testing-internal");
        String password = randomAlphaOfLengthBetween(5, 20);
        ProjectFileSettingsRealm realm = createProjectFileSettingsRealm();
        ProjectId projectId = randomUniqueProjectId();
        setActiveProjectId(projectId);
        addPasswordHashToClusterState(projectId, username, password);

        {
            final PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken(username, new SecureString(password.toCharArray())), authFuture);
            var result = authFuture.get();
            assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        }

        // change password
        String newPassword = password + randomAlphaOfLengthBetween(1, 2);
        addPasswordHashToClusterState(projectId, username, newPassword);

        for (String passwordToTest : shuffledList(List.of(newPassword, password))) {
            final PlainActionFuture<AuthenticationResult<User>> authFuture = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken(username, new SecureString(passwordToTest.toCharArray())), authFuture);
            var result = authFuture.get();
            assertThat(
                result.getStatus(),
                passwordToTest.equals(newPassword) ? is(AuthenticationResult.Status.SUCCESS) : is(AuthenticationResult.Status.CONTINUE)
            );
        }
    }

    public void testSupported() {
        ProjectFileSettingsRealm realm = createProjectFileSettingsRealm();
        assertTrue(realm.supports(new UsernamePasswordToken("test", new SecureString(new char[] {}))));
        assertFalse(realm.supports(mock(AuthenticationToken.class)));
    }

    public void testToken() {
        String username = randomFrom("admin", "testing-internal");
        String password = randomAlphaOfLengthBetween(5, 20);
        ProjectFileSettingsRealm realm = createProjectFileSettingsRealm();

        for (var projectId : randomList(3, 5, ESTestCase::randomUniqueProjectId)) {
            addPasswordHashToClusterState(projectId, username, projectId.id() + password);
            setActiveProjectId(projectId);

            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            UsernamePasswordToken.putTokenHeader(
                threadContext,
                new UsernamePasswordToken(username, new SecureString(password.toCharArray()))
            );

            UsernamePasswordToken token = (UsernamePasswordToken) realm.token(threadContext);
            assertThat(token, notNullValue());
            assertThat(token.principal(), equalTo(username));
            assertThat(token.credentials(), notNullValue());
            assertThat(token.credentials().getChars(), equalTo(password.toCharArray()));
        }
    }

    private void addPasswordHashToClusterState(ProjectId projectId, String user, String password) {
        MockSecureSettings settings = new MockSecureSettings();
        settings.setFile(
            PROJECT_FILE_SETTINGS_PASSWORD_HASH.apply(user).getConcreteSettingForNamespace(REALM_IDENTIFIER.getName()).getKey(),
            hashPasswordForFileAuth(password).getBytes(StandardCharsets.UTF_8)
        );
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

    private static String hashPasswordForFileAuth(String password) {
        return new String(hasher.hash(new SecureString(password.toCharArray())));
    }

    private RealmConfig getRealmConfig(Settings settings) {
        return new RealmConfig(REALM_IDENTIFIER, settings, TestEnvironment.newEnvironment(settings), threadContext);
    }

    private ProjectFileSettingsRealm createProjectFileSettingsRealm() {
        ProjectFileSettingsRealm realm = new ProjectFileSettingsRealm(realmConfig, projectResolver, clusterService);

        realm.setRealmRef(
            new Authentication.RealmRef(REALM_IDENTIFIER.getName(), REALM_IDENTIFIER.getType(), ProjectFileSettingsRealm.class.getName())
        );
        return realm;
    }
}
