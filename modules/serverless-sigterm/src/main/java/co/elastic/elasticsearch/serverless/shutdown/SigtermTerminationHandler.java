/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.serverless.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;

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
        CountDownLatch latch = new CountDownLatch(1);
        client.execute(PutShutdownNodeAction.INSTANCE, shutdownRequest(), ActionListener.wrap(res -> {
            if (res.isAcknowledged()) {
                pollStatusAndLoop(latch);
            } else {
                logger.warn("failed to register graceful shutdown request, request was not acknowledged, stopping immediately");
                latch.countDown();
            }
        }, ex -> {
            logger.warn("failed to register graceful shutdown request with an exception, stopping immediately", ex);
            latch.countDown();
        }));
        try {
            boolean latchReachedZero = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            if (latchReachedZero == false && timeout.millis() != 0) {
                logger.warn("timed out while waiting for shutdown to complete gracefully, shutting down anyway");
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
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
        assert request.validate() == null;
        return request;
    }

    private void pollStatusAndLoop(CountDownLatch latch) {
        client.execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(nodeId), ActionListener.wrap(res -> {
            assert res.getShutdownStatuses().size() == 1 : "got more than this node's shutdown status";
            SingleNodeShutdownStatus status = res.getShutdownStatuses().get(0);
            if (status.overallStatus().equals(SingleNodeShutdownMetadata.Status.COMPLETE)
                || status.overallStatus().equals(SingleNodeShutdownMetadata.Status.STALLED)) {
                logger.info(format("node ready for shutdown with status %s", status.overallStatus()));
                latch.countDown();
            } else {
                logger.debug("polled for shutdown status: {}", status);
                threadPool.schedule(() -> pollStatusAndLoop(latch), pollInterval, ThreadPool.Names.GENERIC);
            }
        }, ex -> {
            logger.warn("failed to get shutdown status for this node while waiting for shutdown, stopping immediately", ex);
            latch.countDown();
        }));
    }
}
