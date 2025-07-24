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

package co.elastic.elasticsearch.test.fixtures.uiam;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.MountableFile;

import java.util.Objects;

/**
 * Test container for UIAM service that integrates with CosmosDB emulator.
 */
public final class UiamServiceTestContainer extends DockerEnvironmentAwareTestContainer {

    /**
     * The port on which the UIAM service is running.
     */
    public static final int UIAM_PORT = 8080;

    private static final String UIAM_IMAGE = "docker.elastic.co/cloud-ci/uiam:git-636fabba8fcf"
        + "@sha256:049a4cfce9a5c159dd1aefbb1c99796ebea5637c03ab3d439683c6ae97e4feb4";
    public static final String JWT_TOKENS_SIGNATURE_SECRET =
        "MnpT2a582F/LiRbocLHLnSF2SYElqTUdmQvBpVn+51Q=,FrsRWeRGTl761r2HihQE/JisZbnemGfR7fUjtUlSouM=";
    public static final String API_KEY_PREFIX = "essu_dev";
    public static final String SHARED_SECRET = "Dw7eRt5yU2iO9pL3aS4dF6gH8jK0lZ1xC2vB3nM4qW5=";

    private final CosmosDbEmulatorTestContainer cosmosDbContainer;

    @SuppressWarnings("this-escape")
    public UiamServiceTestContainer(CosmosDbEmulatorTestContainer cosmosDbContainer) {
        super(new RemoteDockerImage(UIAM_IMAGE));
        this.cosmosDbContainer = Objects.requireNonNull(cosmosDbContainer, "cosmosDbContainer must not be null");
    }

    private void configureContainer() {
        withNetworkMode("container:" + cosmosDbContainer.getContainerName());
        withCopyFileToContainer(MountableFile.forClasspathResource("certs/cosmosdb-root-ca.crt"), "/etc/certs/cosmosdb-root-ca.crt");
        withCopyFileToContainer(
            MountableFile.forClasspathResource("uiam/uiam-entrypoint.sh"),
            "/opt/jboss/container/java/run/uiam-entrypoint.sh"
        );
        withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/sh", "/opt/jboss/container/java/run/uiam-entrypoint.sh"));
        withEnv("quarkus.profile", "dev");
        withEnv("uiam.cosmos.database", cosmosDbContainer.getCosmosDbName());
        withEnv("uiam.cosmos.account.endpoint", cosmosDbContainer.getCosmosDbEndpoint());
        withEnv("uiam.cosmos.account.access_key", cosmosDbContainer.getCosmosDbKey());
        withEnv("uiam.api_keys.encoder.prefix", API_KEY_PREFIX);
        withEnv("uiam.api_keys.decoder.prefixes", API_KEY_PREFIX);
        withEnv("uiam.tokens.jwt.signature.secrets", JWT_TOKENS_SIGNATURE_SECRET);
        withEnv("uiam.internal.shared.secrets", SHARED_SECRET);

        // Connection mode must be set to Gateway in order for UIAM to be able to use the CosmosDB emulator
        withEnv("uiam.cosmos.gateway_connection_mode", "true");

        withEnv("uiam.cosmos.container.apikey", "api-keys");
        withEnv("uiam.cosmos.container.token_invalidation", "token-invalidation");

        withEnv("quarkus.otel.sdk.disabled", "true");
        withEnv("quarkus.http.ssl.certificate.key-store-provider", "JKS");
        withEnv("quarkus.http.ssl.certificate.trust-store-provider", "SUN");

        // Logging
        withEnv("quarkus.log.console.json.enabled", "false");
        withEnv("quarkus.log.level", "DEBUG");
        withEnv("quarkus.log.category.\"io\".level", "DEBUG");
        withEnv("quarkus.log.category.\"co\".level", "DEBUG");
        withEnv("quarkus.log.category.\"org\".level", "DEBUG");
    }

    @Override
    public void start() {
        configureContainer();
        super.start();
    }

    /**
     * @return The URL of the UIAM service.
     */
    public String getUiamUrl() {
        return "http://" + getHost() + ":" + cosmosDbContainer.getMappedPort(UIAM_PORT);
    }

    /**
     * @return The signature secret used to sign JWT tokens.
     */
    public String getTokensSignatureSecret() {
        return JWT_TOKENS_SIGNATURE_SECRET;
    }

    /**
     * @return The shared secret used to authenticate internal API keys.
     */
    public String getSharedSecret() {
        return SHARED_SECRET;
    }
}
