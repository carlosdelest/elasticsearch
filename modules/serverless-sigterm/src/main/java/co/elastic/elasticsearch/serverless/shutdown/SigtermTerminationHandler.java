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

package co.elastic.elasticsearch.serverless.shutdown;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;

/**
 * This class actually implements the logic that's invoked when Elasticsearch receives a sigterm - that is, issuing a Put Shutdown request
 * for this node and periodically checking the Get Shutdown Status API for this node until it's done.
 */
public class SigtermTerminationHandler implements TerminationHandler {
    private static final Logger logger = LogManager.getLogger(SigtermTerminationHandler.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final TimeValue pollInterval;
    private final TimeValue timeout;
    private final String nodeId;

    // Use a fixed timeout for transport actions: we are in a termination handler, there is no way to give clients a way to specify it.
    static final TimeValue SHUTDOWN_STATE_REQUEST_TIMEOUT = TimeValue.timeValueMinutes(1);

    public SigtermTerminationHandler(Client client, ThreadPool threadPool, TimeValue pollInterval, TimeValue timeout, String nodeId) {
        this.client = new OriginSettingClient(client, ClientHelper.STACK_ORIGIN);
        this.threadPool = threadPool;
        this.pollInterval = pollInterval;
        this.timeout = timeout;
        this.nodeId = nodeId;
    }

    @Override
    public void handleTermination() {
        logger.info("handling graceful shutdown request");
        final long started = threadPool.rawRelativeTimeInMillis();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SingleNodeShutdownStatus> lastStatus = new AtomicReference<>();
        AtomicBoolean failed = new AtomicBoolean(false);
        client.execute(
            PutShutdownNodeAction.INSTANCE,
            shutdownRequest(),
            ActionListener.wrap(res -> pollStatusAndLoop(0, latch, lastStatus), ex -> {
                logger.warn("failed to register graceful shutdown request, stopping immediately", ex);
                failed.set(true);
                latch.countDown();
            })
        );
        try {
            boolean latchReachedZero = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            boolean timedOut = latchReachedZero == false && timeout.millis() != 0;
            SingleNodeShutdownStatus status = lastStatus.get();
            if (timedOut && status != null && status.migrationStatus().getShardsRemaining() > 0) {
                logger.info("Timed out waiting for graceful shutdown, retrieving current recoveries status");
                logDetailedRecoveryStatusAndWait();
            }
            var duration = threadPool.rawRelativeTimeInMillis() - started;
            logger.info(
                new ESLogMessage("shutdown completed after [{}] ms with status [{}]", duration, status) //
                    .withFields(
                        Map.of(
                            "elasticsearch.shutdown.status",
                            getShutdownStatus(failed.get(), status),
                            "elasticsearch.shutdown.duration",
                            duration,
                            "elasticsearch.shutdown.timed-out",
                            timedOut
                        )
                    )
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private static String getShutdownStatus(boolean failed, SingleNodeShutdownStatus status) {
        if (failed) {
            return "FAILED";
        } else if (status != null) {
            return status.overallStatus().toString();
        } else {
            return "UNKNOWN";
        }
    }

    private void logDetailedRecoveryStatusAndWait() {
        CountDownLatch latch = new CountDownLatch(1);
        logDetailedRecoveryStatus(latch::countDown);
        try {
            latch.await(); // no need for a timeout, if this takes too long the node will shutdown anyways
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void logDetailedRecoveryStatus(Releasable releasable) {
        var request = new RecoveryRequest();
        request.activeOnly(true);
        client.execute(RecoveryAction.INSTANCE, request, ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RecoveryResponse recoveryResponse) {
                logger.warn("Ongoing recoveries: {}", recoveryResponse);
            }

            @Override
            public void onFailure(Exception ex) {
                logger.error("Failed to get recoveries status", ex);
            }
        }, releasable));
    }

    private PutShutdownNodeAction.Request shutdownRequest() {
        PutShutdownNodeAction.Request request = new PutShutdownNodeAction.Request(
            SHUTDOWN_STATE_REQUEST_TIMEOUT,
            SHUTDOWN_STATE_REQUEST_TIMEOUT,
            nodeId,
            SingleNodeShutdownMetadata.Type.SIGTERM,
            "node sigterm",
            null,
            null,
            timeout
        );
        assert request.validate() == null;
        return request;
    }

    private void pollStatusAndLoop(int poll, CountDownLatch latch, AtomicReference<SingleNodeShutdownStatus> lastStatus) {
        // This transport action does not use a timeout, so we use INFINITE_MASTER_NODE_TIMEOUT as a way to express "this will not time out"
        final var request = new GetShutdownStatusAction.Request(INFINITE_MASTER_NODE_TIMEOUT, nodeId);
        client.execute(GetShutdownStatusAction.INSTANCE, request, ActionListener.wrap(res -> {
            assert res.getShutdownStatuses().size() == 1 : "got more than this node's shutdown status";
            SingleNodeShutdownStatus status = res.getShutdownStatuses().get(0);
            lastStatus.set(status);
            if (status.overallStatus().equals(SingleNodeShutdownMetadata.Status.COMPLETE)) {
                logger.debug("node ready for shutdown with status [{}]: {}", status.overallStatus(), status);
                latch.countDown();
            } else {
                final var level = poll % 10 == 0 ? Level.INFO : Level.DEBUG;
                if (logger.isEnabled(level)) {
                    logger.log(
                        level,
                        new ESLogMessage("polled for shutdown status: {}", status).withFields(
                            Map.of("elasticsearch.shutdown.poll_count", poll)
                        )
                    );
                    if (poll > 0) {
                        HotThreads.logLocalHotThreads(
                            logger,
                            level,
                            "hot threads while waiting for shutdown [poll_count=" + poll + "]",
                            ReferenceDocs.LOGGING
                        );
                        logDetailedRecoveryStatus(() -> {});
                    }
                }
                threadPool.schedule(() -> pollStatusAndLoop(poll + 1, latch, lastStatus), pollInterval, threadPool.generic());
            }
        }, ex -> {
            logger.warn("failed to get shutdown status for this node while waiting for shutdown, stopping immediately", ex);
            latch.countDown();
        }));
    }
}
