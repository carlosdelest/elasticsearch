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

package co.elastic.elasticsearch.serverless.security.logging;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ServerlessUserLoggingDataProviderTests extends ESTestCase {

    private ThreadContext threadContext;
    private SecurityContext securityContext;
    private ServerlessUserLoggingDataProvider provider;

    @Before
    public void setUpProvider() throws Exception {
        this.threadContext = new ThreadContext(Settings.EMPTY);
        this.securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        this.provider = new ServerlessUserLoggingDataProvider(() -> securityContext);
    }

    public void testNoSecurityContextSet() {
        ServerlessUserLoggingDataProvider provider = new ServerlessUserLoggingDataProvider(() -> null);

        Map<String, String> map = new HashMap<>();
        provider.collectData(map);

        assertThat(map, Matchers.anEmptyMap());
    }

    public void testNoAuthentication() {
        SecurityContext securityContextMock = Mockito.mock(SecurityContext.class);
        Mockito.when(securityContextMock.getAuthentication()).thenReturn(null);
        ServerlessUserLoggingDataProvider provider = new ServerlessUserLoggingDataProvider(() -> securityContextMock);

        Map<String, String> map = new HashMap<>();
        provider.collectData(map);

        assertThat(map, Matchers.anEmptyMap());
    }

    public void testLogUser() {
        final String userid = randomAlphaOfLengthBetween(4, 12);
        final String name = randomUnicodeOfLengthBetween(3, 6) + ' ' + randomUnicodeOfLengthBetween(3, 6);
        final String[] roles = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(4, 8));
        final String email = randomAlphaOfLengthBetween(3, 6) + "@" + randomAlphaOfLength(5) + "." + randomAlphaOfLength(3);
        final User user = new User(userid, roles, name, email, Map.of(), true);
        final String realmType = randomAlphaOfLengthBetween(3, 6);
        final String realmName = randomAlphaOfLengthBetween(4, 8);
        final Authentication.RealmRef realm = new Authentication.RealmRef(realmName, realmType, randomAlphaOfLength(5));
        final Authentication authentication = AuthenticationTestHelper.builder().realm(false).user(user).realmRef(realm).build(false);

        final Map<String, String> loggingData = securityContext.executeWithAuthentication(authentication, ignore -> {
            final Map<String, String> map = new LinkedHashMap<>();
            map.put("prefill", "do not overwrite");
            provider.collectData(map);
            return map;
        });

        assertThat(loggingData, Matchers.hasEntry("prefill", "do not overwrite"));
        assertThat(loggingData, Matchers.hasEntry("user.name", userid));
        assertThat(loggingData, Matchers.hasEntry("user.realm", realmName));
        assertThat(loggingData, Matchers.hasEntry("user.full_name", name));
        assertThat(loggingData, Matchers.hasEntry("auth.type", "REALM"));

        // Assert the order
        assertThat(loggingData.keySet(), Matchers.contains("prefill", "user.name", "user.full_name", "user.realm", "auth.type"));
    }

    public void testLogApiKey() {
        final String userid = randomAlphaOfLengthBetween(4, 12);
        final String name = randomUnicodeOfLengthBetween(3, 6) + ' ' + randomUnicodeOfLengthBetween(3, 6);
        final String[] roles = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(4, 8));
        final String email = randomAlphaOfLengthBetween(3, 6) + "@" + randomAlphaOfLength(5) + "." + randomAlphaOfLength(3);
        final User user = new User(userid, roles, name, email, Map.of(), true);
        final String realmType = randomAlphaOfLengthBetween(3, 6);
        final String realmName = randomAlphaOfLengthBetween(4, 8);
        final Authentication.RealmRef realm = new Authentication.RealmRef(realmName, realmType, randomAlphaOfLength(5));
        String apiKeyId = randomUUID();
        final Authentication authentication = AuthenticationTestHelper.builder().user(user).realmRef(realm).apiKey(apiKeyId).build(false);

        final Map<String, String> loggingData = securityContext.executeWithAuthentication(authentication, ignore -> {
            final Map<String, String> map = new LinkedHashMap<>();
            map.put("prefill", "do not overwrite");
            provider.collectData(map);
            return map;
        });

        assertThat(loggingData, Matchers.hasEntry("prefill", "do not overwrite"));
        assertThat(loggingData, Matchers.hasEntry("user.name", userid));
        assertThat(loggingData, Matchers.hasEntry("user.full_name", name));
        assertThat(loggingData, Matchers.hasEntry("apikey.id", apiKeyId));
        assertThat(loggingData, Matchers.hasEntry("auth.type", "API_KEY"));

        // Assert the order
        assertThat(loggingData.keySet(), Matchers.contains("prefill", "user.name", "user.full_name", "apikey.id", "auth.type"));
    }

}
