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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Cloud API key service that handles authentication of cloud API keys against the universal IAM service.
 * It uses the {@link UniversalIamClient} to make requests to the IAM service. It calls the regional UIAM service whose URL is configured
 * via the {@link co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin#UNIVERSAL_IAM_SERVICE_URL_SETTING} setting.
 */
public class CloudApiKeyService implements Closeable {

    private static final Logger logger = LogManager.getLogger(CloudApiKeyService.class);

    private final String nodeName;
    private final UniversalIamClient client;
    private final Supplier<ProjectInfo> projectInfoSupplier;

    public CloudApiKeyService(String nodeName, UniversalIamClient client, Supplier<ProjectInfo> projectInfoSupplier) {
        this.nodeName = nodeName;
        this.client = client;
        this.projectInfoSupplier = projectInfoSupplier;
    }

    /**
     * Authenticates a cloud API key against the universal IAM service.
     *
     * @param cloudApiKey the cloud API key to authenticate.
     * @param listener the listener to notify with the authentication result.
     */
    public void authenticate(CloudApiKey cloudApiKey, ActionListener<Authentication> listener) {
        final ProjectInfo projectInfo = projectInfoSupplier.get();
        final CloudApiKeyAuthenticationRequest authenticationRequest = new CloudApiKeyAuthenticationRequest(projectInfo, cloudApiKey);

        client.authenticateProject(authenticationRequest, ActionListener.wrap(response -> {
            logger.debug("Got response from universal IAM service for authentication with cloud API key [{}]", response);
            // TODO consider failing authentication if returned roles are empty (subject has not effective access to the project)
            final String[] assignedRoles = response.applicationRoles().toArray(new String[0]);
            final User user = new User(
                response.apiKeyId(), // username == cloud API key ID
                assignedRoles,
                null,
                null,
                buildUserMetadata(response),
                true
            );
            final Authentication authentication = Authentication.newCloudApiKeyAuthentication(
                AuthenticationResult.success(user, buildAuthenticationMetadata(response, projectInfo)),
                nodeName
            );
            listener.onResponse(authentication);
        }, ex -> {
            logger.debug("Failed to authenticate cloud API key against universal IAM service", ex);
            if (ex instanceof ElasticsearchSecurityException ese) {
                listener.onFailure(createAuthenticationException(projectInfo.projectId(), ese));
            } else {
                listener.onFailure(createAuthenticationException(projectInfo.projectId()));
            }
        }));
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static Map<String, Object> buildAuthenticationMetadata(
        final CloudApiKeyAuthenticationResponse response,
        final ProjectInfo projectInfo
    ) {
        if (false == projectInfo.organizationId().equals(response.organizationId())) {
            throw new IllegalStateException(
                "organization ID ["
                    + response.organizationId()
                    + "] returned by universal IAM service does not match sent organization ID ["
                    + projectInfo.organizationId()
                    + "]"
            );
        }
        return Map.of(
            CloudAuthenticationFields.AUTHENTICATING_PROJECT_METADATA_KEY,
            Map.ofEntries(
                Map.entry("project_type", projectInfo.projectType()),
                Map.entry("organization_id", projectInfo.organizationId()),
                Map.entry("project_id", projectInfo.projectId())
            )
        );
    }

    private static Map<String, Object> buildUserMetadata(final CloudApiKeyAuthenticationResponse response) {
        if (response.apiKeyDescription() != null) {
            return Map.ofEntries(
                Map.entry(AuthenticationField.API_KEY_INTERNAL_KEY, false),
                Map.entry(AuthenticationField.API_KEY_NAME_KEY, response.apiKeyDescription())
            );
        } else {
            return Map.of(AuthenticationField.API_KEY_INTERNAL_KEY, false);
        }
    }

    private static ElasticsearchSecurityException createAuthenticationException(String projectId) {
        return createAuthenticationException(projectId, null);
    }

    private static ElasticsearchSecurityException createAuthenticationException(
        String projectId,
        @Nullable ElasticsearchSecurityException ex
    ) {
        return new ElasticsearchSecurityException(
            Strings.format("failed to authenticate cloud API key for project [%s]", projectId),
            RestStatus.UNAUTHORIZED,
            ex
        );
    }
}
