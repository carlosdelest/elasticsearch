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

package co.elastic.elasticsearch.settings.secure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.file.MasterNodeFileWatchingService;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class FileSecureSettingsService extends MasterNodeFileWatchingService {

    private static final Logger logger = LogManager.getLogger(FileSecureSettingsService.class);

    private final Environment environment;
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<SecretsUpdateTask> updateQueue;
    private final MasterServiceTaskQueue<ErrorUpdateTask> errorQueue;

    public FileSecureSettingsService(ClusterService clusterService, Environment environment) {
        super(clusterService, LocallyMountedSecrets.resolveSecretsFile(environment));
        this.clusterService = clusterService;
        this.updateQueue = clusterService.createTaskQueue("secure_settings", Priority.NORMAL, new SecretsTaskExecutor());
        this.errorQueue = clusterService.createTaskQueue("secure_settings_errors", Priority.NORMAL, new ErrorTaskExecutor());
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
        LocallyMountedSecrets secrets = null;
        try {
            secrets = new LocallyMountedSecrets(environment);

            clusterService.getClusterSettings().validate(Settings.builder().setSecureSettings(secrets).build(), true);

            settings = new ClusterStateSecrets(secrets.getVersion(), secrets);
            metadata = ClusterStateSecretsMetadata.createSuccessful(secrets.getVersion());
            process(settings, metadata);
        } catch (Exception e) {
            ClusterStateSecretsMetadata previousMetadata = clusterService.state().custom(ClusterStateSecretsMetadata.TYPE);
            logger.warn("Error reading secure settings:", e);
            metadata = ClusterStateSecretsMetadata.createError(secrets == null ? -1 : secrets.getVersion(), getStackTraceAsList(e));

            if (previousMetadata == null || metadata.getVersion() != previousMetadata.getVersion()) {
                processError(metadata);
            }
        }
    }

    @Override
    protected void processInitialFileMissing() {
        // TODO: this should add an empty cluster state for secure settings state, but
        // until readiness allows plugging in additional ready state hooks there is no need
    }

    private static List<String> getStackTraceAsList(Exception e) {
        return List.of(ExceptionsHelper.stackTrace(e));
    }

    public void processError(ClusterStateSecretsMetadata metadata) {
        ErrorUpdateTask task = new ErrorUpdateTask(metadata);
        errorQueue.submitTask("file-secure-settings[error]", task, TimeValue.timeValueSeconds(30L));
    }

    public void process(ClusterStateSecrets settings, ClusterStateSecretsMetadata metadata) {
        ClusterState initialState = clusterService.state();
        ClusterStateSecretsMetadata existingMetadata = initialState.custom(ClusterStateSecretsMetadata.TYPE);

        if (existingMetadata != null && metadata.getVersion() <= existingMetadata.getVersion()) {
            return;
        }
        logger.debug("Updating secure settings to version {}", metadata.getVersion());
        SecretsUpdateTask task = new SecretsUpdateTask(settings, metadata, this::processError);
        updateQueue.submitTask("file-secure-settings[version=" + metadata.getVersion() + "]", task, TimeValue.timeValueSeconds(30L));
    }

    private static class SecretsUpdateTask extends MetadataHoldingUpdateTask {
        private final ClusterStateSecrets settings;
        private final ClusterStateSecretsMetadata metadata;
        private final Consumer<ClusterStateSecretsMetadata> errorHandler;

        SecretsUpdateTask(
            ClusterStateSecrets settings,
            ClusterStateSecretsMetadata metadata,
            Consumer<ClusterStateSecretsMetadata> errorHandler
        ) {
            this.settings = settings;
            this.metadata = metadata;
            this.errorHandler = errorHandler;
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
            errorHandler.accept(ClusterStateSecretsMetadata.createError(metadata.getVersion(), List.of(ExceptionsHelper.stackTrace(e))));
        }

        ClusterStateSecretsMetadata getMetadata() {
            return this.metadata;
        }
    }

    // package-private for testing - exposes metadata for tests
    abstract static class MetadataHoldingUpdateTask extends ClusterStateUpdateTask {
        abstract ClusterStateSecretsMetadata getMetadata();
    }

    private static class ErrorUpdateTask extends MetadataHoldingUpdateTask {
        private final ClusterStateSecretsMetadata metadata;

        ErrorUpdateTask(ClusterStateSecretsMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            if (this.metadata.equals(currentState.custom(ClusterStateSecretsMetadata.TYPE)) == false) {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.putCustom(ClusterStateSecretsMetadata.TYPE, metadata);
                return builder.build();
            }
            return currentState;
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Failed to add error to cluster state metadata", e);
        }

        @Override
        ClusterStateSecretsMetadata getMetadata() {
            return this.metadata;
        }
    }

    private static class SecretsTaskExecutor implements ClusterStateTaskExecutor<SecretsUpdateTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<SecretsUpdateTask> batchExecutionContext) {
            List<? extends TaskContext<SecretsUpdateTask>> contexts = batchExecutionContext.taskContexts();
            ClusterState initialState = batchExecutionContext.initialState();

            Long maxSettingsVersion = null;
            Optional<SecretsUpdateTask> task = Optional.empty();
            for (TaskContext<SecretsUpdateTask> context : contexts) {
                Long version = context.getTask().getMetadata().getVersion();
                if (maxSettingsVersion == null || version.compareTo(maxSettingsVersion) > 0) {
                    maxSettingsVersion = version;
                    task = Optional.of(context.getTask());
                }
                context.success(() -> {});
            }

            if (task.isPresent()) {
                try {
                    return task.get().execute(batchExecutionContext.initialState());
                } catch (Exception e) {
                    task.get().errorHandler.accept(
                        ClusterStateSecretsMetadata.createError(task.get().metadata.getVersion(), getStackTraceAsList(e))
                    );
                }
            }
            return initialState;
        }
    }

    private static class ErrorTaskExecutor extends SimpleBatchedExecutor<ErrorUpdateTask, Void> {

        @Override
        public Tuple<ClusterState, Void> executeTask(ErrorUpdateTask errorUpdateTask, ClusterState clusterState) throws Exception {
            return Tuple.tuple(errorUpdateTask.execute(clusterState), null);
        }

        @Override
        public void taskSucceeded(ErrorUpdateTask errorUpdateTask, Void unused) {
            // do nothing
        }
    }
}
