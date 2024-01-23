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

package co.elastic.elasticsearch.serverless.snapshots.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static java.util.stream.Collectors.joining;

/**
 * Captures the workflow for <b>destructively</b> restoring an entire project from a snapshot in a single high-level convenience API.
 * <p>
 * At a very high level, this can be thought of as an in-Elasticsearch realization of the
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshots-restore-snapshot.html#restore-entire-cluster">Restore
 * an entire cluster</a> instructions (filtered to only the components that are relevant to serverless).
 * <p>
 * It consists of three fundamental operations:
 * <ol>
 * <li>Prepare the cluster and cluster settings for restore.</li>
 * <li>Delete all data streams and indices (this is the <b>destructive</b> part!).</li>
 * <li>Restore a snapshot.</li>
 * </ol>
 * <p>
 * Since all it does is call existing Elasticsearch APIs, its functionality is limited by capabilities of those APIs. Similarly, if a
 * limitation of this API is found, it may be useful to 'just' skip the high level API that this provides and instead execute the relevant
 * underlying operations yourself using the lower level APIs.
 * <p>
 * One feature this API provides over the underlying snapshot API is that it will look up a "most recent successful snapshot" for you,
 * which you can ask it to do by specifying <code>_latest_success</code> as the snapshot to restore.
 */
public class TransportRestoreProjectAction extends TransportAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {

    public static final ActionType<RestoreSnapshotResponse> TYPE = ActionType.localOnly("cluster:admin/snapshot/project_restore");

    private static final Logger logger = LogManager.getLogger(TransportRestoreProjectAction.class);

    // this constant is inspired by GetSnapshotsRequest.CURRENT_SNAPSHOT which has a value of "_current",
    // and also the ESS version of this which is "__latest_success__" (but that doesn't seem especially Elasticsearch-y)
    public static final String LATEST_SUCCESSFUL_SNAPSHOT = "_latest_success";

    private final Client client;
    private final Executor restoreExecutor;
    private final ThreadContext threadContext;

    @Inject
    public TransportRestoreProjectAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager());
        this.client = client;
        final var threadPool = transportService.getThreadPool();
        this.restoreExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        this.threadContext = threadPool.getThreadContext();
    }

    private static <Response> ActionListener<Response> ignoreResult(ActionListener<Void> listener) {
        return listener.map(ignored -> null);
    }

    /**
     * Looks up the most recent successful snapshot in the snapshot repository and sets it into the RestoreSnapshotRequest as the
     * snapshot to restore.
     */
    private void lookupLatestSuccessfulSnapshot(RestoreSnapshotRequest request, ActionListener<Void> listener) {
        final String repository = request.repository();
        logger.info("looking up most recent successful snapshot in repository [{}]", repository);

        client.admin()
            .cluster()
            .prepareGetSnapshots()
            .setRepositories(repository)
            .setSort(GetSnapshotsRequest.SortBy.START_TIME)
            .setOrder(SortOrder.ASC)
            .setVerbose(false) // note: verbose=false *requires* that the order be START_TIME + ASC, made explicit above for clarity
            .execute(listener.map(response -> {
                // since they're in *ascending* order, iterate backwards until you find a successful snapshot
                String latestSuccessfulSnapshot = null;
                for (int i = response.getSnapshots().size() - 1; i >= 0; i--) {
                    SnapshotInfo info = response.getSnapshots().get(i);
                    if (info.state().equals(SnapshotState.SUCCESS)) {
                        latestSuccessfulSnapshot = info.snapshotId().getName();
                        break;
                    }
                }

                if (latestSuccessfulSnapshot != null) {
                    final String name = latestSuccessfulSnapshot;
                    logger.info("the most recent successful snapshot in repository [{}] was [{}]", repository, name);
                    request.snapshot(name); // note: evil! banging on a mutable reference!
                    return null;
                } else {
                    final var error = Strings.format("no successful snapshots in repository [%s]", repository);
                    logger.error(error);
                    throw new IllegalArgumentException(error);
                }
            }));
    }

    /**
     * Verifies that snapshot specified by the RestoreSnapshotRequest exists (and is either a successful or partial snapshot).
     */
    private void verifySnapshotExists(RestoreSnapshotRequest request, ActionListener<Void> listener) {
        final String repository = request.repository();
        final String snapshot = request.snapshot();
        logger.info("snapshot name [{}] was provided, verifying that it exists in repository [{}]", snapshot, repository);

        client.admin()
            .cluster()
            .prepareGetSnapshots()
            .setRepositories(repository)
            .setSnapshots(Strings.splitStringByCommaToArray(snapshot)) // n.b. handle a list of snapshots, providing a more precise error
            .setVerbose(false)
            .execute(listener.map(response -> {
                final int count = response.getSnapshots().size();
                if (count == 0) {
                    // note: this should never happen -- we'd get an error response and not a success response from the server,
                    // but it doesn't harm us to belt and suspenders that in case something changes elsewhere.
                    final var error = Strings.format("did not find snapshot [%s] in repository [%s]", snapshot, repository);
                    logger.error(error);
                    throw new IllegalArgumentException(error);
                } else if (count > 1) {
                    // we do not support wildcards or comma separated snapshot names, don't try to be sneaky
                    final var names = response.getSnapshots().stream().map(info -> info.snapshotId().getName()).collect(joining(","));
                    final var error = Strings.format(
                        "found more than one snapshot [%s] matching [%s] in repository [%s]",
                        names,
                        snapshot,
                        repository
                    );
                    logger.error(error);
                    throw new IllegalArgumentException(error);
                } else {
                    final var name = response.getSnapshots()
                        .stream()
                        .filter(info -> info.state().equals(SnapshotState.SUCCESS) || info.state().equals(SnapshotState.PARTIAL))
                        .findFirst()
                        .orElseThrow()
                        .snapshotId()
                        .getName();
                    if (name.equals(snapshot) == false) {
                        // we do not support wildcards or comma separated snapshot names, don't try to be sneaky -- even if a wildcard
                        // pattern matches just a single snapshot, you have to name the exact snapshot. no tricks.
                        final var error = Strings.format(
                            "found snapshot [%s] in repository [%s] but it didn't match provided snapshot [%s]",
                            name,
                            repository,
                            snapshot
                        );
                        logger.error(error);
                        throw new IllegalArgumentException(error);
                    } else {
                        // all checks have succeeded, and there's no reason to object to the provided snapshot name
                        return null;
                    }
                }
            }));
    }

    /**
     * Enables or disables the geoip downloader.
     */
    private void setGeoIpDownloaderEnabled(boolean enabled, ActionListener<Void> listener) {
        if (enabled) {
            logger.info("enabling geoip downloader");
        } else {
            logger.info("disabling geoip downloader");
        }

        // the geoip downloader defaults to enabled, so we explicitly disable it, but then implicitly re-enable it
        Settings.Builder enabledSettings = enabled
            ? Settings.builder().putNull("ingest.geoip.downloader.enabled")
            : Settings.builder().put("ingest.geoip.downloader.enabled", false);
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(enabledSettings)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .execute(ignoreResult(listener));
    }

    /**
     * Enables or disables universal profiling index management.
     */
    private void setProfilingIndexManagementEnabled(boolean enabled, ActionListener<Void> listener) {
        if (enabled) {
            logger.info("enabling profiling index management");
        } else {
            logger.info("disabling profiling index management");
        }
        // Universal Profiling index management defaults to enabled on Serverless, so we explicitly disable it,
        // but then implicitly re-enable it
        Settings.Builder enabledSettings = enabled
            ? Settings.builder().putNull("xpack.profiling.templates.enabled")
            : Settings.builder().put("xpack.profiling.templates.enabled", false);
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(enabledSettings)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .execute(ignoreResult(listener));
    }

    /**
     * Enables or disables APM index management.
     */
    private void setAPMIndexManagementEnabled(boolean enabled, ActionListener<Void> listener) {
        if (enabled) {
            logger.info("enabling APM index management");
        } else {
            logger.info("disabling APM index management");
        }
        // APM index management defaults to enabled if the xpack apm-data plugin is
        // enabled, which is the case on Serverless; so we explicitly disable it,
        // but then implicitly re-enable it
        Settings.Builder enabledSettings = enabled
            ? Settings.builder().putNull("xpack.apm_data.registry.enabled")
            : Settings.builder().put("xpack.apm_data.registry.enabled", false);
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(enabledSettings)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .execute(ignoreResult(listener));
    }

    /**
     * Enables or disables ML upgrade mode.
     */
    private void setMlUpgradeMode(boolean enabled, ActionListener<Void> listener) {
        if (enabled) {
            logger.info("enabling ml upgrade mode");
        } else {
            logger.info("disabling ml upgrade mode");
        }

        SetUpgradeModeAction.Request mlEnableUpgradeModeRequest = new SetUpgradeModeAction.Request(enabled);
        mlEnableUpgradeModeRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
        client.execute(SetUpgradeModeAction.INSTANCE, mlEnableUpgradeModeRequest, ignoreResult(listener));
    }

    /**
     * Enables or disables wildcard-ed destructive actions.
     */
    private void setDestructiveRequiresName(boolean destructiveRequiresName, ActionListener<Void> listener) {
        if (destructiveRequiresName) {
            logger.info("disabling wildcard destructive actions");
        } else {
            logger.info("enabling wildcard destructive actions");
        }

        // by default, we require a name for destructive operations, so we explicitly disable it, but then implicitly re-enable it
        Settings.Builder enabledSettings = destructiveRequiresName
            ? Settings.builder().putNull("action.destructive_requires_name")
            : Settings.builder().put("action.destructive_requires_name", false);

        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(enabledSettings)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .execute(ignoreResult(listener));
    }

    @Override
    protected void doExecute(Task task, RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
        // some operations in the andThen chain below can (and need to be) undone when the project restoration is complete (regardless of
        // whether it succeeds or fails). this list allows each operation to record its associated undo operation. all undo operations are
        // executed at the end of the restore. note: this shouldn't actually need to be a synchronized list, but it might seem surprising
        // that it doesn't need to be given all the async code around it -- this synchronization is just to give the reader a security
        // blanket.
        final List<Consumer<ActionListener<Void>>> undoOperations = Collections.synchronizedList(new ArrayList<>());

        final String repository = request.repository();
        final String snapshot = request.snapshot();

        logger.info("request received to restore [{}]/[{}]", repository, snapshot);

        restoreExecutor.execute(
            ActionRunnable.wrap(
                listener,
                restoreListener -> SubscribableListener

                    // look up or validate the snapshot details
                    .<Void>newForked(l -> {
                        if (snapshot.equals(LATEST_SUCCESSFUL_SNAPSHOT)) {
                            lookupLatestSuccessfulSnapshot(request, l);
                        } else {
                            verifySnapshotExists(request, l);
                        }
                    })

                    // disable the geoip downloader
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        setGeoIpDownloaderEnabled(false, l);
                        undoOperations.add(l2 -> setGeoIpDownloaderEnabled(true, l2));
                    })

                    // put ml into upgrade mode
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        setMlUpgradeMode(true, l);
                        undoOperations.add(l2 -> setMlUpgradeMode(false, l2));
                    })

                    // disable universal profiling
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        setProfilingIndexManagementEnabled(false, l);
                        undoOperations.add(l2 -> setProfilingIndexManagementEnabled(true, l2));
                    })

                    // disable apm_data registry
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        setAPMIndexManagementEnabled(false, l);
                        undoOperations.add(l2 -> setAPMIndexManagementEnabled(true, l2));
                    })

                    // enable destructive actions by wildcard
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        setDestructiveRequiresName(false, l);
                        undoOperations.add(l2 -> setDestructiveRequiresName(true, l2));
                    })

                    // delete all data streams
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        logger.info("deleting data streams!");
                        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("*");
                        deleteDataStreamRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
                        deleteDataStreamRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
                        client.execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest, ignoreResult(l));
                    })

                    // delete all indices
                    .<Void>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        logger.info("deleting indices!");
                        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
                        deleteIndexRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
                        deleteIndexRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
                        client.admin().indices().delete(deleteIndexRequest, ignoreResult(l));
                    })

                    // restore the snapshot
                    .<RestoreSnapshotResponse>andThen(restoreExecutor, threadContext, (l, ignored) -> {
                        logger.info("restoring snapshot!");
                        client.admin().cluster().restoreSnapshot(request, l);
                    })

                    // finally invoke the restore listener to signal that we're done, calling the undo operations beforehand
                    .addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(RestoreSnapshotResponse restoreSnapshotResponse) {
                            try (var l = new RefCountingListener(restoreListener.map(ignored -> restoreSnapshotResponse))) {
                                undoOperations.forEach(o -> ActionListener.run(l.acquire(), o::accept));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try (var l = new RefCountingListener(new ActionListener<>() {
                                @Override
                                public void onResponse(Void ignored) {
                                    restoreListener.onFailure(e);
                                }

                                @Override
                                public void onFailure(Exception e2) {
                                    e.addSuppressed(e2);
                                    restoreListener.onFailure(e);
                                }
                            })) {
                                undoOperations.forEach(o -> ActionListener.run(l.acquire(), o::accept));
                            }
                        }
                    })
            )
        );
    }
}
