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

package co.elastic.elasticsearch.serverless.security.cloud;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CloudApiKeyServiceTests extends ESTestCase {

    private static final String ORGANIZATION_ID = "test-org-id";
    private static final String PROJECT_ID = "test-project-id";
    private static final ProjectType PROJECT_TYPE = ProjectType.ELASTICSEARCH_GENERAL_PURPOSE;
    private static final String NODE_ID = "test-node-id";
    private static final String API_KEY_ID = "test-api-key_id";
    private static final String API_KEY_NAME = "test-api-key-name";

    private final Supplier<ProjectInfo> projectInfoSupplier = () -> new ProjectInfo(PROJECT_ID, ORGANIZATION_ID, PROJECT_TYPE);

    public void testSuccessfulAuthentication() {
        final SecureString sharedSecret = randomBoolean() ? new SecureString("shared-secret".toCharArray()) : null;
        final boolean internal = sharedSecret != null;
        final List<String> roles = List.of("viewer", "editor");
        final UniversalIamClient client = mockUniversalIamClient(roles, internal);
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        final CloudApiKeyService cloudApiKeyService = new CloudApiKeyService(NODE_ID, client, projectInfoSupplier);

        cloudApiKeyService.authenticate(new CloudApiKey(new SecureString("essu_test-api-key".toCharArray()), sharedSecret), future);

        Authentication authentication = future.actionGet();
        assertThat(authentication, notNullValue());
        assertThat(authentication.isCloudApiKey(), equalTo(true));
        assertThat(authentication.getAuthenticationType(), equalTo(Authentication.AuthenticationType.API_KEY));
        assertThat(authentication.getAuthenticatingSubject(), equalTo(authentication.getEffectiveSubject()));
        assertThat(authentication.getAuthenticatingSubject().getUser().principal(), equalTo(API_KEY_ID));
        assertThat(authentication.getAuthenticatingSubject().getUser().roles(), arrayContainingInAnyOrder(roles.toArray()));

        assertThat(
            authentication.getAuthenticatingSubject().getUser().metadata(),
            equalTo(
                Map.ofEntries(
                    Map.entry(AuthenticationField.API_KEY_NAME_KEY, API_KEY_NAME),
                    Map.entry(AuthenticationField.API_KEY_INTERNAL_KEY, internal)
                )
            )
        );

        assertThat(
            authentication.getAuthenticatingSubject().getMetadata(),
            equalTo(
                Map.of(
                    CloudAuthenticationFields.AUTHENTICATING_PROJECT_METADATA_KEY,
                    Map.ofEntries(
                        Map.entry("project_type", "elasticsearch"),
                        Map.entry("organization_id", ORGANIZATION_ID),
                        Map.entry("project_id", PROJECT_ID)
                    )
                )
            )
        );
    }

    public void testFailedAuthentication() {
        final UniversalIamClient client = mockUniversalIamClient(new RuntimeException("test exception"));
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        final CloudApiKeyService cloudApiKeyService = new CloudApiKeyService(NODE_ID, client, projectInfoSupplier);
        final SecureString sharedSecret = randomBoolean() ? new SecureString("shared-secret".toCharArray()) : null;

        cloudApiKeyService.authenticate(new CloudApiKey(new SecureString("essu_test-api-key".toCharArray()), sharedSecret), future);

        // TODO: Should be changed once we improve error handling
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(ese.getMessage(), equalTo("failed to authenticate cloud API key for project [" + PROJECT_ID + "]"));
        assertThat(ese.status(), equalTo(RestStatus.UNAUTHORIZED));
    }

    public void testNoRolesAfterSuccessfulAuthentication() {
        final SecureString sharedSecret = randomBoolean() ? new SecureString("shared-secret".toCharArray()) : null;
        final boolean internal = sharedSecret != null;
        final UniversalIamClient client = mockUniversalIamClient(Collections.emptyList(), internal);
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        final CloudApiKeyService cloudApiKeyService = new CloudApiKeyService(NODE_ID, client, projectInfoSupplier);

        cloudApiKeyService.authenticate(new CloudApiKey(new SecureString("essu_test-api-key".toCharArray()), sharedSecret), future);

        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(ese.getMessage(), equalTo("failed to authorize cloud API key for project [" + PROJECT_ID + "]"));
        assertThat(ese.status(), equalTo(RestStatus.FORBIDDEN));
    }

    @SuppressWarnings("unchecked")
    private UniversalIamClient mockUniversalIamClient(List<String> roles, boolean internal) {
        UniversalIamClient client = mock(UniversalIamClient.class);
        doAnswer(invocation -> {
            CloudApiKeyAuthenticationResponse response = new CloudApiKeyAuthenticationResponse(
                API_KEY_ID,
                ORGANIZATION_ID,
                List.of(new CloudAuthenticateProjectContext(new ProjectInfo(PROJECT_ID, ORGANIZATION_ID, "elasticsearch"), roles)),
                new CloudCredentialsMetadata(
                    "api-key",
                    internal,
                    Instant.now(),
                    randomBoolean() ? Instant.now().plus(Duration.ofDays(7)) : null
                ),
                "api-key",
                API_KEY_NAME
            );
            ((ActionListener<CloudApiKeyAuthenticationResponse>) invocation.getArgument(1)).onResponse(response);
            return null;
        }).when(client).authenticateProject(any(CloudApiKeyAuthenticationRequest.class), any(ActionListener.class));
        return client;
    }

    @SuppressWarnings("unchecked")
    private UniversalIamClient mockUniversalIamClient(Exception e) {
        UniversalIamClient client = mock(UniversalIamClient.class);
        doAnswer(invocation -> {
            ((ActionListener<CloudApiKeyAuthenticationResponse>) invocation.getArgument(1)).onFailure(e);
            return null;
        }).when(client).authenticateProject(any(CloudApiKeyAuthenticationRequest.class), any(ActionListener.class));
        return client;
    }
}
