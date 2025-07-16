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
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Path;

/**
 * Python  container which is used to bootstrap the CosmosDB emulator and insert test API keys from a JSON file.
 */
final class CosmosDbInitContainer extends DockerEnvironmentAwareTestContainer {

    private final CosmosDbEmulatorTestContainer cosmosDbEmulatorContainer;

    /**
     * Creates and configures the init container that will bootstrap the database with API keys.
     */
    CosmosDbInitContainer(CosmosDbEmulatorTestContainer cosmosDbEmulatorContainer, Path apiKeysPath) {
        super(
            new ImageFromDockerfile().withDockerfileFromBuilder(
                builder -> builder.from("python")
                    .copy("/api-keys.json", "/api-keys.json")
                    .copy("/init-cosmos.sh", "/init-cosmos.sh")
                    .run("chmod +x /init-cosmos.sh")
                    .entryPoint("/init-cosmos.sh")
                    .build()
            ).withFileFromPath("/api-keys.json", apiKeysPath).withFileFromClasspath("/init-cosmos.sh", "cosmos-init/init-cosmos.sh")
        );
        this.cosmosDbEmulatorContainer = cosmosDbEmulatorContainer;
    }

    private void configureContainer() {
        this.withNetworkMode("container:" + cosmosDbEmulatorContainer.getContainerName())
            .withEnv("COSMOS_ENDPOINT", cosmosDbEmulatorContainer.getCosmosDbEndpoint())
            .withEnv("COSMOS_KEY", cosmosDbEmulatorContainer.getCosmosDbKey())
            .withEnv("COSMOS_DATABASE", cosmosDbEmulatorContainer.getCosmosDbName());
    }

    @Override
    public void start() {
        configureContainer();
        super.start();
    }
}
