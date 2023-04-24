/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.settings.secure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.file.AbstractFileWatchingService;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class FileSecureSettingsService extends AbstractFileWatchingService {

    private static final Logger logger = LogManager.getLogger(FileSecureSettingsService.class);

    private final Environment environment;

    public FileSecureSettingsService(ClusterService clusterService, Environment environment) {
        super(clusterService, LocallyMountedSecrets.resolveSecretsFile(environment));
        this.environment = environment;
    }

    /**
     * Read settings
     *
     * @throws IOException if there is an error reading the file itself
     * @throws ExecutionException if there is an issue while applying the changes from the file
     * @throws InterruptedException if the file processing is interrupted by another thread.
     */
    @Override
    protected void processFileChanges() throws InterruptedException, ExecutionException, IOException {
        try (LocallyMountedSecrets secrets = new LocallyMountedSecrets(environment)) {
            logger.info("Secure setting keys: {}", secrets.getSettingNames());
        }
        ;
    }
}
