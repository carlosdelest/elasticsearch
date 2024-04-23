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
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
        client.execute(
            PutShutdownNodeAction.INSTANCE,
            shutdownRequest(),
            ActionListener.wrap(res -> pollStatusAndLoop(0, latch, lastStatus), ex -> {
                var duration = threadPool.rawRelativeTimeInMillis() - started;
                logger.warn(
                    new ESLogMessage("failed to register graceful shutdown request, stopping immediately") //
                        .withFields(createShutdownFields("FAILED", duration, false)),
                    ex
                );
                latch.countDown();
            })
        );
        try {
            boolean latchReachedZero = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            boolean timedOut = latchReachedZero == false && timeout.millis() != 0;
            SingleNodeShutdownStatus status = lastStatus.get();
            if (timedOut && status != null && status.migrationStatus().getShardsRemaining() > 0) {
                logDetailedRecoveryStatus();
            }
            var duration = threadPool.rawRelativeTimeInMillis() - started;
            logger.info(
                new ESLogMessage("shutdown completed after [{}] ms with status [{}]", duration, status) //
                    .withFields(createShutdownFields(status != null ? status.overallStatus().toString() : null, duration, timedOut))
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private static Map<String, Object> createShutdownFields(String status, long duration, boolean timedOut) {
        return Map.of(
            "elasticsearch.shutdown.status",
            Objects.toString(status),
            "elasticsearch.shutdown.duration",
            duration,
            "elasticsearch.shutdown.timed-out",
            timedOut
        );
    }

    private void logDetailedRecoveryStatus() {
        logger.info("Timed out waiting for graceful shutdown, retrieving current recoveries status");

        CountDownLatch latch = new CountDownLatch(1);
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
        }, latch::countDown));

        try {
            latch.await(); // no need for a timeout, if this takes too long the node will shutdown anyways
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private PutShutdownNodeAction.Request shutdownRequest() {
        PutShutdownNodeAction.Request request = new PutShutdownNodeAction.Request(
            nodeId,
            SingleNodeShutdownMetadata.Type.SIGTERM,
            "node sigterm",
            null,
            null,
            timeout
        );
        request.masterNodeTimeout(timeout);
        request.ackTimeout(timeout);
        assert request.validate() == null;
        return request;
    }

    private void pollStatusAndLoop(int poll, CountDownLatch latch, AtomicReference<SingleNodeShutdownStatus> lastStatus) {
        final var request = new GetShutdownStatusAction.Request(nodeId);
        request.masterNodeTimeout(timeout);
        client.execute(GetShutdownStatusAction.INSTANCE, request, ActionListener.wrap(res -> {
            assert res.getShutdownStatuses().size() == 1 : "got more than this node's shutdown status";
            SingleNodeShutdownStatus status = res.getShutdownStatuses().get(0);
            lastStatus.set(status);
            if (status.overallStatus().equals(SingleNodeShutdownMetadata.Status.COMPLETE)) {
                logger.debug("node ready for shutdown with status [{}]: {}", status.overallStatus(), status);
                latch.countDown();
            } else {
                logger.log(poll % 10 == 0 ? Level.INFO : Level.DEBUG, "polled for shutdown status: {}", status);
                threadPool.schedule(() -> pollStatusAndLoop(poll + 1, latch, lastStatus), pollInterval, threadPool.generic());
            }
        }, ex -> {
            logger.warn("failed to get shutdown status for this node while waiting for shutdown, stopping immediately", ex);
            latch.countDown();
        }));
    }
}
