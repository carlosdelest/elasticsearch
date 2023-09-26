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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetrics;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction.Request;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetAutoscalingMetricsAction.Response;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IndexTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchTierMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class TransportGetAutoscalingMetricsAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingMetricsAction.class);

    // some constants controlling timeouts
    private static final int MIN_TIMEOUT_PER_METRIC_MS = 100;
    private static final double PER_METRIC_TIMEOUT_SHARE = 0.8;

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetAutoscalingMetricsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            GetAutoscalingMetricsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        var parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        final AtomicReference<IndexTierMetrics> indexTierMetricsRef = new AtomicReference<>();
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<MachineLearningTierMetrics> machineLearningMetricsRef = new AtomicReference<>();

        ActionListener<Void> tierResponsesListener = listener.map(
            unused -> new Response(indexTierMetricsRef.get(), searchTierMetricsRef.get(), machineLearningMetricsRef.get())
        );
        var countDownListener = new CountDownActionListener(3, tierResponsesListener);

        TimeValue timeoutPerMetric = TimeValue.timeValueMillis(
            Math.max((long) (request.timeout().millis() * PER_METRIC_TIMEOUT_SHARE), MIN_TIMEOUT_PER_METRIC_MS)
        );

        // execute requests for every tier, note: log in debug to not flood the production log with stack traces
        executeRequest(
            parentTaskAssigningClient,
            threadPool,
            GetIndexTierMetrics.INSTANCE,
            new GetIndexTierMetrics.Request(timeoutPerMetric),
            ActionListener.wrap(response -> indexTierMetricsRef.set(response.getMetrics()), e -> {
                logger.warn("failed to retrieve index tier metrics", e);
                indexTierMetricsRef.set(new IndexTierMetrics(getFailureReason(e), wrapExceptionIfNecessary(e)));
            }),
            countDownListener
        );
        executeRequest(
            parentTaskAssigningClient,
            threadPool,
            GetSearchTierMetrics.INSTANCE,
            new GetSearchTierMetrics.Request(timeoutPerMetric),
            ActionListener.wrap(response -> searchTierMetricsRef.set(response.getMetrics()), e -> {
                logger.warn("failed to retrieve search tier metrics", e);
                searchTierMetricsRef.set(new SearchTierMetrics(getFailureReason(e), wrapExceptionIfNecessary(e)));
            }),
            countDownListener
        );
        executeRequest(
            parentTaskAssigningClient,
            threadPool,
            GetMachineLearningTierMetrics.INSTANCE,
            new GetMachineLearningTierMetrics.Request(timeoutPerMetric),
            ActionListener.wrap(response -> machineLearningMetricsRef.set(response.getMetrics()), e -> {
                logger.warn("failed to retrieve ml tier metrics", e);
                machineLearningMetricsRef.set(new MachineLearningTierMetrics(getFailureReason(e), wrapExceptionIfNecessary(e)));
            }),
            countDownListener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    static <Req extends AbstractTierMetricsRequest<Req>, Resp extends ActionResponse> void executeRequest(
        Client client,
        ThreadPool threadPool,
        ActionType<Resp> action,
        Req request,
        ActionListener<Resp> listener,
        CountDownActionListener countDownActionListener
    ) {
        ClientHelper.executeAsyncWithOrigin(
            client,
            // TODO: we might use our own origin
            ClientHelper.STACK_ORIGIN,
            action,
            request,
            ListenerTimeouts.wrapWithTimeout(
                threadPool,
                request.timeout(),
                threadPool.generic(),
                ActionListener.runAfter(listener, () -> countDownActionListener.onResponse(null)),
                (ignore) -> {
                    listener.onFailure(new ElasticsearchTimeoutException("timed out after [" + request.timeout() + "]"));
                    countDownActionListener.onResponse(null);
                }
            )
        );
    }

    private static String getFailureReason(Exception e) {
        if (e instanceof TimeoutException timeoutException) {
            return timeoutException.getMessage();
        }
        return "failed to retrieve metrics";
    }

    private static ElasticsearchException wrapExceptionIfNecessary(Exception e) {
        if (e instanceof ElasticsearchException elasticsearchException) {
            return elasticsearchException;
        }
        return new ElasticsearchException("failed to retrieve metrics", e);
    }
}
