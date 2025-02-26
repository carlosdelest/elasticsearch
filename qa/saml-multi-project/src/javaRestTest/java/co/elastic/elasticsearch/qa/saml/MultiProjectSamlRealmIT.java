/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package co.elastic.elasticsearch.qa.saml;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.authc.saml.SamlIdpMetadataBuilder;
import org.elasticsearch.xpack.security.authc.saml.SamlResponseBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MultiProjectSamlRealmIT extends ESRestTestCase {

    public static ElasticsearchCluster cluster = initTestCluster();
    private static Path caPath;

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(cluster);

    private static final String IDP_ENTITY_ID = "https://idp.example.org/";
    private static final String MULTI_REALM_NAME = "multi-saml";
    private static final String SINGLE_REALM_NAME = "single-saml";
    private static final List<String> PROJECT_IDS = List.of("mjolnir", "gungnir", "brisingamen");

    private static ElasticsearchCluster initTestCluster() {
        return ServerlessElasticsearchCluster.local()
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .setting("xpack.security.http.ssl.enabled", "true")
            .setting("xpack.security.http.ssl.certificate", "node.crt")
            .setting("xpack.security.http.ssl.key", "node.key")
            .setting("xpack.security.http.ssl.certificate_authorities", "ca.crt")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.certificate", "node.crt")
            .setting("xpack.security.transport.ssl.key", "node.key")
            .setting("xpack.security.transport.ssl.certificate_authorities", "ca.crt")
            .setting("xpack.security.transport.ssl.verification_mode", "certificate")
            .setting("serverless.multi_project.enabled", "true")
            .keystore("bootstrap.password", "x-pack-test-password")
            .user("test_admin", "x-pack-test-password", User.ROOT_USER_ROLE, true)
            .user("rest_test", "rest_password", User.ROOT_USER_ROLE, true)
            .configFile("node.key", Resource.fromClasspath("ssl/node.key"))
            .configFile("node.crt", Resource.fromClasspath("ssl/node.crt"))
            .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
            .configFile("metadata/metadata.xml", Resource.fromString(getIDPMetadata()))
            .settings(node -> {
                var settings = new HashMap<String, String>();
                String singlePrefix = "xpack.security.authc.realms.saml." + SINGLE_REALM_NAME;
                settings.put(singlePrefix + ".order", "1");
                settings.put(singlePrefix + ".idp.entity_id", IDP_ENTITY_ID);
                settings.put(singlePrefix + ".idp.metadata.path", "metadata/metadata.xml");
                settings.put(singlePrefix + ".attributes.principal", "urn:oid:2.5.4.3");
                settings.put(singlePrefix + ".ssl.certificate_authorities", "ca.crt");
                settings.put(singlePrefix + ".sp.entity_id", "https://sp/default.example.org/");
                settings.put(singlePrefix + ".sp.acs", "https://acs/default");
                settings.put(singlePrefix + ".sp.logout", "http://logout/default");

                var multiPrefix = "xpack.security.authc.realms.multi_project_saml." + MULTI_REALM_NAME;
                settings.put(multiPrefix + ".order", "0");
                settings.put(multiPrefix + ".idp.entity_id", IDP_ENTITY_ID);
                settings.put(multiPrefix + ".idp.metadata.path", "metadata/metadata.xml");
                settings.put(multiPrefix + ".attributes.principal", "urn:oid:2.5.4.3");
                settings.put(multiPrefix + ".ssl.certificate_authorities", "ca.crt");

                for (String projectId : PROJECT_IDS) {
                    var projectPrefix = "xpack.security.authc.projects." + projectId;
                    settings.put(projectPrefix + ".sp.entity_id", "https://sp/" + projectId + ".example.org/");
                    settings.put(projectPrefix + ".sp.acs", "https://acs/" + projectId);
                    settings.put(projectPrefix + ".sp.logout", "http://logout/" + projectId);
                }
                return settings;
            })
            .build();
    }

    private static String getIDPMetadata() {
        try {
            var signingCert = PathUtils.get(MultiProjectSamlRealmIT.class.getResource("/saml/signing.crt").toURI());
            return new SamlIdpMetadataBuilder().entityId(IDP_ENTITY_ID).idpUrl(IDP_ENTITY_ID).sign(signingCert).asString();
        } catch (URISyntaxException | CertificateException | IOException exception) {
            fail(exception);
        }
        return null;
    }

    @BeforeClass
    public static void loadCertificateAuthority() throws Exception {
        URL resource = MultiProjectSamlRealmIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        caPath = PathUtils.get(resource.toURI());
    }

    @Before
    public void initProjects() {
        PROJECT_IDS.forEach(this::createProject);
    }

    @After
    public void deleteProjects() {
        PROJECT_IDS.forEach(this::deleteProject);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("rest_test", new SecureString("rest_password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(CERTIFICATE_AUTHORITIES, caPath).build();
    }

    public void testAuthenticationSuccessful() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        for (String projectId : shuffledList(PROJECT_IDS)) {
            samlAuthForProject(projectId, username, getSamlAssertionJsonBodyString(projectId, username));
        }
    }

    public void testAuthenticationFailsForWrongProjectId() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var assertionProjectId = randomFrom(PROJECT_IDS);
        var headerProjectId = randomFrom(PROJECT_IDS.stream().filter(p -> p.equals(assertionProjectId) == false).toList());

        var ex = expectThrows(
            ResponseException.class,
            () -> samlAuthForProject(headerProjectId, username, getSamlAssertionJsonBodyString(assertionProjectId, username))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testAuthenticationFailsForUnknownProjectId() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var assertionProjectId = randomFrom(PROJECT_IDS);
        var headerProjectId = randomFrom("this-is-not-a-project", null);

        var ex = expectThrows(
            ResponseException.class,
            () -> samlAuthForProject(headerProjectId, username, getSamlAssertionJsonBodyString(assertionProjectId, username))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    public void testDynamicallyAddAndRemoveProject() throws Exception {
        final String projectId = "gram";
        var projectPrefix = "xpack.security.authc.projects." + projectId;
        final String username = randomAlphaOfLengthBetween(4, 12);

        createProject(projectId);
        {
            var req = requestWithProjectHeader("PUT", "/_cluster/settings");
            final Map<String, Object> body = Map.of(
                "persistent",
                Map.of(
                    projectPrefix + ".sp.entity_id",
                    "https://sp/" + projectId + ".example.org/",
                    projectPrefix + ".sp.acs",
                    "https://acs/" + projectId,
                    projectPrefix + ".sp.logout",
                    "http://logout/" + projectId
                )
            );
            req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
            adminClient().performRequest(req);

            samlAuthForProject(projectId, username, getSamlAssertionJsonBodyString(projectId, username));
        }
        {
            var req = requestWithProjectHeader("PUT", "/_cluster/settings");
            Map<String, Object> settings = new HashMap<>();
            settings.put(projectPrefix + ".sp.entity_id", null);
            settings.put(projectPrefix + ".sp.acs", null);
            settings.put(projectPrefix + ".sp.logout", null);
            final Map<String, Object> body = Map.of("persistent", settings);
            req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
            adminClient().performRequest(req);
            var ex = expectThrows(
                ResponseException.class,
                () -> samlAuthForProject(projectId, username, getSamlAssertionJsonBodyString(projectId, username))
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));
        }

    }

    public void testPrepareSuccessful() throws Exception {
        var projectId = randomFrom(PROJECT_IDS);
        samlPrepareForProject(projectId);
    }

    public void testPrepareFailsForUnknownProjectId() {
        var projectId = randomFrom("this-is-not-a-project", null);
        var ex = expectThrows(ResponseException.class, () -> samlPrepareForProject(projectId));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(400));
    }

    public void testAuthenticationSuccessfulSingleSamlRealm() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var req = requestWithProjectHeader("POST", "_security/saml/authenticate");
        req.setJsonEntity(getSamlAssertionJsonBodyString("default", username, SINGLE_REALM_NAME));
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp.get("username"), equalTo(username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo(SINGLE_REALM_NAME));
    }

    private static Request requestWithProjectHeader(String method, String endpoint) {
        return requestWithProjectHeader(method, endpoint, null);
    }

    private static Request requestWithProjectHeader(String method, String endpoint, @Nullable String projectId) {
        final Request request = new Request(method, endpoint);
        request.setOptions(
            request.getOptions().toBuilder().addHeader("X-Elastic-Project-Id", projectId == null ? "default" : projectId).build()
        );
        return request;
    }

    private String getSamlAssertionJsonBodyString(String projectId, String username) throws Exception {
        return getSamlAssertionJsonBodyString(projectId, username, MULTI_REALM_NAME);
    }

    private String getSamlAssertionJsonBodyString(String projectId, String username, String realmName) throws Exception {
        var message = new SamlResponseBuilder().spEntityId("https://sp/" + projectId + ".example.org/")
            .idpEntityId(IDP_ENTITY_ID)
            .acs(new URL("https://acs/" + projectId))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath("/saml/signing.crt"), getDataPath("/saml/signing.key"), new char[0])
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        body.put("realm", realmName);
        return Strings.toString(JsonXContent.contentBuilder().map(body));
    }

    private void samlAuthForProject(@Nullable String projectId, String username, String samlAssertion) throws Exception {
        var req = requestWithProjectHeader("POST", "_security/saml/authenticate", projectId);
        req.setJsonEntity(samlAssertion);
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp.get("username"), equalTo(username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo(MULTI_REALM_NAME));
    }

    private void samlPrepareForProject(@Nullable String projectId) throws Exception {
        var req = requestWithProjectHeader("POST", "_security/saml/prepare", projectId);
        req.setJsonEntity("{\"realm\": \"" + MULTI_REALM_NAME + "\"}");
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp.keySet(), containsInAnyOrder("realm", "redirect", "id"));
        assertThat(resp.get("redirect").toString(), startsWith(IDP_ENTITY_ID));
        assertEquals(resp.get("realm"), MULTI_REALM_NAME);
    }

    private void createProject(String projectId) {
        final Request request = new Request("PUT", "/_project/" + projectId);
        try {
            adminClient().performRequest(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteProject(String project) {
        final Request request = new Request("DELETE", "/_project/" + project);
        try {
            adminClient().performRequest(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FixForMultiProject(description = "Enable when reset feature states work with multi-project")
    @Override
    protected boolean resetFeatureStates() {
        return false;
    }
}
