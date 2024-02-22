/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.sso;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.idp.authc.AuthenticationMethod;
import org.elasticsearch.xpack.idp.authc.NetworkControl;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.elasticsearch.xpack.idp.saml.authn.SuccessfulAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.UserServiceAuthentication;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.saml2.core.NameID;
import org.w3c.dom.Element;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;

/**
 * This integration test attempts to mimic a typical authentication flow for Serverless projects.
 * Specifically:
 *  - The cluster is configured with SAML role mapping using {@link org.elasticsearch.reservedstate.service.FileSettingsService}
 *  - A user is authenticated using SAML (mimicking the security cluster IdP)
 *  - The user's SAML access token is used to create an API key
 *  - The API key is used to access the ES API
 */
public final class ServerlessSsoIT extends ESRestTestCase {

    private static final String ADMIN_USER = "admin-user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("x-pack-test-password".toCharArray());

    private static final String SP_ENTITY_ID = "ec:/organizations:0123456789/project-elasticsearch:project-abcdef";
    private static final String SP_ACS = "https://project-abcdef.kb.region.csp.elastic.cloud/api/security/saml/callback";
    private static final String SP_LOGOUT = "https://project-abcdef.kb.region.csp.elastic.cloud/logout";
    private static final String PRINCIPAL_ATTRIBUTE = "http://saml.elastic-cloud.com/attributes/principal";
    private static final String NAME_ATTRIBUTE = "http://saml.elastic-cloud.com/attributes/name";
    private static final String EMAIL_ATTRIBUTE = "http://saml.elastic-cloud.com/attributes/email";
    private static final String GROUPS_ATTRIBUTE = "http://saml.elastic-cloud.com/attributes/roles";

    private static final String IDP_ENTITY_ID = "urn:idp-cloud-elastic-co";
    private static final String REALM_NAME = "cloud-saml-kibana";
    private static final Set<String> RESERVED_ROLES = Set.of("superuser", "kibana_admin");
    private static final Set<String> PREDEFINED_ROLES = Set.of("viewer", "editor", "admin", "developer");
    private static final SetOnce<Set<String>> EXCLUDED_ROLES = new SetOnce<>();

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .nodes(2)
        .settings(ServerlessSsoIT::samlRealm)
        .setting("xpack.security.authc.token.enabled", "true")
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .configFile("metadata.xml", Resource.fromClasspath("saml/metadata.xml"))
        .configFile("operator/settings.json", Resource.fromClasspath("operator/role-mappings.json"))
        .configFile("roles.yml", Resource.fromClasspath("roles.yml"))
        // Logging enabled to debug https://github.com/elastic/elasticsearch-serverless/issues/898
        .setting("logger.org.elasticsearch.xpack.security.authc.support.mapper", "TRACE")
        .build();

    private SamlFactory samlFactory;

    /**
     * This mimics the configuration set by the k8s controller
     */
    private static Map<String, String> samlRealm(LocalClusterSpec.LocalNodeSpec node) {
        final var prefix = "xpack.security.authc.realms.saml." + REALM_NAME;
        final Map<String, String> settings = Map.ofEntries(
            Map.entry(prefix + ".order", "101"),
            Map.entry(prefix + ".idp.entity_id", IDP_ENTITY_ID),
            Map.entry(prefix + ".idp.metadata.path", "metadata.xml"),
            Map.entry(prefix + ".sp.entity_id", SP_ENTITY_ID),
            Map.entry(prefix + ".sp.acs", SP_ACS),
            Map.entry(prefix + ".sp.logout", SP_LOGOUT),
            Map.entry(prefix + ".attributes.principal", PRINCIPAL_ATTRIBUTE),
            Map.entry(prefix + ".attributes.name", NAME_ATTRIBUTE),
            Map.entry(prefix + ".attributes.groups", GROUPS_ATTRIBUTE),
            Map.entry(prefix + ".nameid_format", "urn:oasis:names:tc:SAML:2.0:nameid-format:transient")
        );
        EXCLUDED_ROLES.trySet(randomBoolean() ? Set.of() : RESERVED_ROLES);
        if (EXCLUDED_ROLES.get().isEmpty()) {
            return settings;
        } else {
            return Maps.copyMapWithAddedEntry(
                settings,
                prefix + ".exclude_roles",
                Strings.collectionToCommaDelimitedString(EXCLUDED_ROLES.get())
            );
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void setUpObjects() throws Exception {
        samlFactory = new SamlFactory();
    }

    private record UserInfo(String principal, String name, String email, String[] groups) {}

    public void testSamlAuthenticationAndAccess() throws Exception {
        waitForSearchableSecurityIndex();
        waitForRoleMapping("elastic-cloud-sso-kibana-do-not-change"); // from role-mappings.json

        final UserInfo user = randomSamlUser();
        final Map<String, Object> responseBody = samlAuthenticate(user);
        assertAuthentication(responseBody, user);

        String token = (String) responseBody.get("access_token");
        verifyAccessToken(token, user);

        var indexName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        var apiKeyName = randomAlphaOfLength(12);
        String encodedApiKey = createApiKeyFromToken(apiKeyName, token, indexName);

        verifyApiKey(encodedApiKey, apiKeyName, user);
        verifySearchWithApiKey(indexName, encodedApiKey);

        invalidateApiKey(encodedApiKey);
        verifyApiKeyInvalidated(encodedApiKey);

        invalidateAccessToken(token);
        verifyAccessTokenInvalidated(token);
    }

    private void waitForSearchableSecurityIndex() throws Exception {
        assertBusy(() -> {
            ensureHealth(adminClient(), ".security", req -> {
                ignoreWarnings(req);
                req.addParameter("wait_for_status", "yellow");
                req.addParameter("wait_for_no_initializing_shards", "true");
            });

            try {
                final Request request = new Request("GET", "/.security/_count");
                ignoreWarnings(request);
                adminClient().performRequest(request);
            } catch (ResponseException e) {
                logger.info("Failed to count docs in security index ({})", e.toString());
                final Response response = adminClient().performRequest(new Request("GET", "/_cat/shards"));
                logger.info("GET /_cat/shards:\n{}", EntityUtils.toString(response.getEntity()));
                fail("Failed to count docs in security index");
            }
        }, 45, TimeUnit.SECONDS);
    }

    private void waitForRoleMapping(String name) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "/_security/role_mapping/" + name);
                adminClient().performRequest(request);
            } catch (ResponseException e) {
                logger.info("Failed to find role mapping [{}] - {}", name, e.toString());
                fail("Failed to find role mapping " + name);
            }
        }, 20, TimeUnit.SECONDS);
    }

    private static void ignoreWarnings(Request req) {
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
    }

    private Map<String, Object> samlAuthenticate(UserInfo user) throws Exception {
        final SamlServiceProvider sp = buildServiceProvider();
        final UserServiceAuthentication authentication = new UserServiceAuthentication(
            user.principal(),
            user.name(),
            user.email(),
            Set.copyOf(Arrays.asList(user.groups())),
            sp,
            Set.of(AuthenticationMethod.PASSWORD),
            Set.of(NetworkControl.TLS)
        );

        return samlAuthenticate(authentication);
    }

    private Map<String, Object> samlAuthenticate(UserServiceAuthentication authentication) throws Exception {
        final SuccessfulAuthenticationResponseMessageBuilder builder = getAuthenticationResponseBuilder();

        final var authResponse = builder.build(authentication, null);
        assertThat(authResponse, notNullValue());

        final Element element = XMLObjectSupport.marshall(authResponse);
        final StringWriter writer = new StringWriter();
        SamlFactory.getHardenedXMLTransformer().transform(new DOMSource(element), new StreamResult(writer));
        final String xml = writer.toString();
        logger.info("SAML Authentication Response: {}", xml);

        final String jsonBody = toJson(
            Map.ofEntries(
                Map.entry("content", Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8))),
                Map.entry("realm", REALM_NAME)
            )
        );

        final Request request = new Request("POST", "/_security/saml/authenticate");
        request.setJsonEntity(jsonBody);
        final Map<String, Object> responseBody = entityAsMap(adminClient().performRequest(request));
        return responseBody;
    }

    private static SamlServiceProvider buildServiceProvider() throws Exception {
        final var projectName = "Cloud Serverless Project #" + randomIntBetween(1_000, 9_999);
        final ServiceProviderPrivileges privileges = new ServiceProviderPrivileges(projectName, randomAlphaOfLength(3), ignore -> Set.of());
        final SamlServiceProvider.AttributeNames attributes = new SamlServiceProvider.AttributeNames(
            PRINCIPAL_ATTRIBUTE,
            NAME_ATTRIBUTE,
            EMAIL_ATTRIBUTE,
            GROUPS_ATTRIBUTE
        );
        return new CloudServiceProvider(
            SP_ENTITY_ID,
            projectName,
            true,
            new URL(SP_ACS),
            NameID.TRANSIENT,
            Duration.ofMinutes(randomIntBetween(3, 10)),
            privileges,
            attributes,
            Collections.emptySet(),
            true,
            true
        );
    }

    private static void assertAuthentication(Map<String, Object> responseBody, UserInfo user) throws IOException {
        assertThat(ObjectPath.evaluate(responseBody, "username"), is(user.principal()));
        assertThat(ObjectPath.evaluate(responseBody, "realm"), is(REALM_NAME));
        assertThat(ObjectPath.evaluate(responseBody, "access_token"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "refresh_token"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "expires_in"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "authentication"), notNullValue());
        final Object roles = ObjectPath.evaluate(responseBody, "authentication.roles");
        assertThat(roles, instanceOf(Collection.class));
        assertThat("Roles: " + roles, (Collection<?>) roles, not(containsInAnyOrder(RESERVED_ROLES)));
        assertThat("Roles: " + roles, (Collection<?>) roles, containsInAnyOrder((Object[]) getFilteredUserGroups(user)));
        assertThat(ObjectPath.evaluate(responseBody, "authentication.full_name"), is(user.name()));
    }

    private static void verifyAccessToken(String accessToken, UserInfo user) throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        setBearerAuthentication(accessToken, request);
        final Map<String, Object> responseBody = responseAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(responseBody, "username"), is(user.principal));
        assertThat(ObjectPath.evaluate(responseBody, "full_name"), is(user.name));
        final Object roles = ObjectPath.evaluate(responseBody, "roles");
        assertThat(roles, instanceOf(Collection.class));
        assertThat("Roles: " + roles, (Collection<?>) roles, not(containsInAnyOrder(RESERVED_ROLES)));
        assertThat("Roles: " + roles, (Collection<?>) roles, containsInAnyOrder((Object[]) getFilteredUserGroups(user)));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is(REALM_NAME));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("saml"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("token"));
        assertThat(ObjectPath.evaluate(responseBody, "metadata.saml_principal"), contains(user.principal));
    }

    private static String[] getFilteredUserGroups(UserInfo user) {
        return Arrays.stream(user.groups).filter(g -> EXCLUDED_ROLES.get().contains(g) == false).toArray(String[]::new);
    }

    private void verifyAccessTokenInvalidated(String accessToken) throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        setBearerAuthentication(accessToken, request);
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private String createApiKeyFromToken(String apiKeyName, String accessToken, String withAccessToIndex) throws Exception {
        final Request request = new Request("POST", "/_security/api_key");
        setBearerAuthentication(accessToken, request);
        request.setJsonEntity(Strings.format("""
            {
                "name": "%s",
                "expiration": "1d",
                "role_descriptors": {
                    "view_test_index": {
                        "cluster": [ "manage_own_api_key" ],
                        "indices": [ { "names": [ "%s" ], "privileges": [ "read" ] } ]
                    }
                }
            }""", apiKeyName, withAccessToIndex));

        var responseBody = entityAsMap(client().performRequest(request));
        var encoded = responseBody.get("encoded");
        assertThat("From response: " + responseBody, encoded, notNullValue());
        assertThat(encoded, instanceOf(String.class));
        return (String) encoded;
    }

    private void verifyApiKey(String encodedApiKey, String apiKeyName, UserInfo owner) throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        setApiKeyAuthentication(encodedApiKey, request);
        final Map<String, Object> responseBody = responseAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(responseBody, "username"), is(owner.principal));
        assertThat(ObjectPath.evaluate(responseBody, "full_name"), is(owner.name));
        assertThat(ObjectPath.evaluate(responseBody, "roles"), empty());
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.name"), is(apiKeyName));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.id"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "metadata.saml_principal"), contains(owner.principal));
    }

    private void verifyApiKeyInvalidated(String encodedApiKey) throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        setApiKeyAuthentication(encodedApiKey, request);
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private void verifySearchWithApiKey(String indexName, String encodedApiKey) throws Exception {
        final Request request = new Request("GET", "/" + indexName + "/_count");
        setApiKeyAuthentication(encodedApiKey, request);
        final Map<String, Object> responseBody = responseAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(responseBody, "count"), is(0));
    }

    private void invalidateApiKey(String encodedApiKey) throws Exception {
        final String[] apiKeyParts = new String(Base64.getDecoder().decode(encodedApiKey), StandardCharsets.UTF_8).split(":");
        assertThat(apiKeyParts, arrayWithSize(2));
        var apiKeyId = apiKeyParts[0];

        final Request request = new Request("DELETE", "/_security/api_key/");
        setApiKeyAuthentication(encodedApiKey, request);
        request.setJsonEntity(Strings.format("""
            {
                "ids": [ "%s" ]
            }""", apiKeyId));
        final Map<String, Object> responseBody = responseAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(responseBody, "invalidated_api_keys"), contains(apiKeyId));
    }

    private void invalidateAccessToken(String accessToken) throws Exception {
        final Request request = new Request("DELETE", "/_security/oauth2/token");
        request.setJsonEntity(Strings.format("""
            {
                "token": "%s"
            }""", accessToken));
        final Map<String, Object> responseBody = responseAsMap(adminClient().performRequest(request));
        assertThat(ObjectPath.evaluate(responseBody, "invalidated_tokens"), is(1));
    }

    private static void setBearerAuthentication(String accessToken, Request request) {
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "Bearer " + accessToken));
    }

    private static void setApiKeyAuthentication(String encodedKey, Request request) {
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + encodedKey));
    }

    private static UserInfo randomSamlUser() {
        final var principal = String.valueOf(randomLongBetween(1_000_000_000L, 9_999_999_999L));
        final var name = String.join(" ", randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        final var email = randomAlphaOfLengthBetween(3, 8)
            + "@"
            + randomAlphaOfLengthBetween(2, 6)
            + ".example."
            + randomFrom("net", "com", "org");
        final List<String> predefinedRoles = randomNonEmptySubsetOf(PREDEFINED_ROLES);
        final List<String> reservedRoles = randomSubsetOf(EXCLUDED_ROLES.get());
        final var roles = CollectionUtils.concatLists(predefinedRoles, reservedRoles);
        return new UserInfo(principal, name, email, roles.toArray(String[]::new));
    }

    private SuccessfulAuthenticationResponseMessageBuilder getAuthenticationResponseBuilder() throws Exception {
        final List<Certificate> certificates = PemUtils.readCertificates(Set.of(getDataPath("/saml/cloud-idp.crt")));
        assertThat(certificates, hasSize(1));
        X509Certificate cert = (X509Certificate) certificates.get(0);

        final PrivateKey key = PemUtils.readPrivateKey(getDataPath("/saml/cloud-idp.key"), () -> null);

        final SamlIdentityProvider idp = SamlIdentityProvider.builder(null, null)
            .entityId(IDP_ENTITY_ID)
            .singleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI, new URL("https://console.fake.cld.elstc.co/sso/v1/saml"))
            .signingCredential(new org.opensaml.security.x509.BasicX509Credential(cert, key))
            .serviceProviderDefaults(
                new ServiceProviderDefaults(randomAlphaOfLength(2), NameID.TRANSIENT, Duration.ofMinutes(randomIntBetween(1, 10)))
            )
            .allowedNameIdFormat(NameID.TRANSIENT)
            .build();
        return new SuccessfulAuthenticationResponseMessageBuilder(samlFactory, Clock.systemUTC(), idp);
    }

    private static String toJson(Map<String, ? extends Object> map) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().map(map);
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }

}
