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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.util.Objects;

import static co.elastic.elasticsearch.test.fixtures.uiam.UiamServiceTestContainer.UIAM_PORT;

/**
 * Test container which Azure CosmosDB Emulator with initialization support.
 * This container manages the lifecycle of the CosmosDB emulator and initializes it with API keys from a JSON file.
 */
public final class CosmosDbEmulatorTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String COSMOS_DB_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private static final String COSMOS_DB_IMAGE = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview";
    private static final String COSMOS_DB_DATABASE = "uiam";
    private static final int COSMOS_DB_PORT = 8081;

    private final CosmosDbInitContainer initContainer;

    /**
     * Creates a new CosmosDB emulator container with the default network and API keys path.
     *
     * @param apiKeysPath Path to API keys JSON file
     */
    public CosmosDbEmulatorTestContainer(Path apiKeysPath) {
        this(Network.newNetwork(), apiKeysPath);
    }

    /**
     * Creates a new CosmosDB emulator container with the specified network and API keys path.
     *
     * @param network     Docker network to use
     * @param apiKeysPath Path to API keys JSON file
     */
    @SuppressWarnings("this-escape")
    public CosmosDbEmulatorTestContainer(Network network, Path apiKeysPath) {
        super(new RemoteDockerImage(COSMOS_DB_IMAGE));
        Objects.requireNonNull(network, "network must not be null");
        Objects.requireNonNull(apiKeysPath, "apiKeysPath must not be null");
        this.initContainer = new CosmosDbInitContainer(this, apiKeysPath);

        this.withNetwork(network)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("certs/cosmos-db-certificate.pfx"),
                "/scripts/certs/cosmos-db-certificate.pfx"
            )
            .withCommand("--protocol", "https", "--port", String.valueOf(COSMOS_DB_PORT))
            .withEnv("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", "1")
            .withEnv("AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE", "false")
            .withEnv("ENABLE_TELEMETRY", "false")
            .withEnv("ENABLE_EXPLORER", "false")
            .withEnv("LOG_LEVEL", "debug")
            .withEnv("CERT_PATH", "/scripts/certs/cosmos-db-certificate.pfx")
            .withEnv("CERT_SECRET", "secret")
            .withExposedPorts(COSMOS_DB_PORT, UIAM_PORT)
            .waitingFor(Wait.forListeningPorts(COSMOS_DB_PORT));
    }

    @Override
    public void start() {
        super.start();
        initContainer.start();
    }

    @Override
    public void stop() {
        super.stop();
        initContainer.stop();
    }

    /**
     * @return the CosmosDB endpoint URL
     */
    public String getCosmosDbEndpoint() {
        return "https://" + getHost() + ":" + COSMOS_DB_PORT;
    }

    /**
     * @return the CosmosDB key
     */
    public String getCosmosDbKey() {
        return COSMOS_DB_KEY;
    }

    /**
     * @return the CosmosDB database name
     */
    public String getCosmosDbName() {
        return COSMOS_DB_DATABASE;
    }

}
