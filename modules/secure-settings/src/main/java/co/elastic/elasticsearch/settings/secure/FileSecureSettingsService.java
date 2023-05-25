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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.file.AbstractFileWatchingService;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class FileSecureSettingsService extends AbstractFileWatchingService {

    private static final Logger logger = LogManager.getLogger(FileSecureSettingsService.class);

    private final Environment environment;
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<UpdateTask> updateQueue;

    public FileSecureSettingsService(ClusterService clusterService, Environment environment) {
        super(clusterService, LocallyMountedSecrets.resolveSecretsFile(environment));
        this.clusterService = clusterService;
        this.updateQueue = clusterService.createTaskQueue("secure_settings", Priority.NORMAL, new TaskExecutor());
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
        ClusterStateSecrets settings;
        ClusterStateSecretsMetadata metadata;
        try {
            LocallyMountedSecrets secrets = new LocallyMountedSecrets(environment);

            clusterService.getClusterSettings().validate(Settings.builder().setSecureSettings(secrets).build(), true);

            settings = new ClusterStateSecrets(secrets.getVersion(), secrets);
            metadata = ClusterStateSecretsMetadata.createSuccessful(secrets.getVersion());
            process(settings, metadata);
        } catch (Exception e) {
            ClusterStateSecretsMetadata previousMetadata = clusterService.state().custom(ClusterStateSecretsMetadata.TYPE);
            metadata = ClusterStateSecretsMetadata.createError(
                previousMetadata != null ? previousMetadata.getVersion() : -1,
                Stream.concat(Stream.of(e.getMessage()), Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString)).toList()
            );
            processError(metadata);
            logger.warn("Error reading secure settings:", e);
        }
    }

    public void processError(ClusterStateSecretsMetadata metadata) {
        ClusterState initialState = clusterService.state();
        ClusterStateSecrets existingSettings = initialState.custom(ClusterStateSecrets.TYPE);
        UpdateTask task = new UpdateTask(existingSettings, metadata);
        updateQueue.submitTask("file-secure-settings[error]", task, TimeValue.timeValueSeconds(30L));
    }

    public void process(ClusterStateSecrets settings, ClusterStateSecretsMetadata metadata) {
        ClusterState initialState = clusterService.state();
        ClusterStateSecrets existingMetadata = initialState.custom(ClusterStateSecrets.TYPE);

        if (existingMetadata != null && metadata.getVersion() <= existingMetadata.getVersion()) {
            return;
        }
        logger.debug("Updating secure settings to version {}", metadata.getVersion());
        UpdateTask task = new UpdateTask(settings, metadata);
        updateQueue.submitTask("file-secure-settings[version=" + metadata.getVersion() + "]", task, TimeValue.timeValueSeconds(30L));
    }

    private static class UpdateTask extends ClusterStateUpdateTask {
        private final ClusterStateSecrets settings;
        private final ClusterStateSecretsMetadata metadata;

        UpdateTask(ClusterStateSecrets settings, ClusterStateSecretsMetadata metadata) {
            this.settings = settings;
            this.metadata = metadata;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            builder.putCustom(ClusterStateSecretsMetadata.TYPE, metadata);
            if (this.settings != null) {
                builder.putCustom(ClusterStateSecrets.TYPE, settings);
            }
            return builder.build();
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Failed to update cluster state for secure settings", e);
        }

        private ClusterStateSecretsMetadata getMetadata() {
            return this.metadata;
        }
    }

    private static class TaskExecutor implements ClusterStateTaskExecutor<UpdateTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<UpdateTask> batchExecutionContext) throws Exception {
            List<? extends TaskContext<UpdateTask>> contexts = batchExecutionContext.taskContexts();
            ClusterState initialState = batchExecutionContext.initialState();

            Long maxSettingsVersion = null;
            Optional<UpdateTask> task = Optional.empty();
            for (TaskContext<UpdateTask> context : contexts) {
                Long version = context.getTask().getMetadata().getVersion();
                if (maxSettingsVersion == null || version.compareTo(maxSettingsVersion) > 0) {
                    maxSettingsVersion = version;
                    task = Optional.of(context.getTask());
                }
                context.success(() -> {});
            }

            if (task.isPresent()) {
                return task.get().execute(batchExecutionContext.initialState());
            }
            return initialState;
        }
    }
}
