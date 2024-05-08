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

package co.elastic.elasticsearch.metering.action;

import co.elastic.elasticsearch.metering.MeteringIndexInfoTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.NotPersistentTaskNodeException;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static co.elastic.elasticsearch.metering.MeteringIndexInfoTaskExecutor.MINIMUM_METERING_INFO_UPDATE_PERIOD;
import static co.elastic.elasticsearch.metering.MeteringIndexInfoTaskExecutor.POLL_INTERVAL_SETTING;
import static co.elastic.elasticsearch.metering.action.utils.PersistentTaskUtils.findPersistentTaskNode;

abstract class TransportGetMeteringStatsAction extends HandledTransportAction<
    GetMeteringStatsAction.Request,
    GetMeteringStatsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetMeteringStatsAction.class);

    static final long MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS = (long) (MINIMUM_METERING_INFO_UPDATE_PERIOD.millis() * 0.75);
    static final long MINIMUM_INITIAL_BACKOFF_PERIOD_MILLIS = (long) (MINIMUM_METERING_INFO_UPDATE_PERIOD.millis() * 0.02);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MeteringIndexInfoService meteringIndexInfoService;
    private final ExecutorService executor;
    private final TransportService transportService;
    private final String persistentTaskName;

    private volatile TimeValue meteringShardInfoUpdatePeriod;

    TransportGetMeteringStatsAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MeteringIndexInfoService meteringIndexInfoService
    ) {
        this(
            actionName,
            transportService,
            actionFilters,
            clusterService,
            indexNameExpressionResolver,
            meteringIndexInfoService,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings())
        );

        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setMeteringShardInfoUpdatePeriod);
    }

    TransportGetMeteringStatsAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MeteringIndexInfoService meteringIndexInfoService,
        ExecutorService executor,
        TimeValue meteringShardInfoUpdatePeriod
    ) {
        super(actionName, transportService, actionFilters, GetMeteringStatsAction.Request::new, executor);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.meteringIndexInfoService = meteringIndexInfoService;
        this.executor = executor;
        this.persistentTaskName = MeteringIndexInfoTask.TASK_NAME;
        this.meteringShardInfoUpdatePeriod = meteringShardInfoUpdatePeriod;
    }

    void setMeteringShardInfoUpdatePeriod(TimeValue meteringShardInfoUpdatePeriod) {
        this.meteringShardInfoUpdatePeriod = meteringShardInfoUpdatePeriod;
    }

    /**
     * We want to give the transport action as much time as it is sensible to execute before timing out, as long as we are returning valid
     * (up-to-date) information. For this reason, we link the timeout to the persistent task polling interval.
     */
    TimeValue getPersistentTaskNodeTransportActionTimeout() {
        var transportActionTimeout = (long) (meteringShardInfoUpdatePeriod.millis() * 0.75);
        if (transportActionTimeout < MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS) {
            transportActionTimeout = MINIMUM_TRANSPORT_ACTION_TIMEOUT_MILLIS;
        }
        return TimeValue.timeValueMillis(transportActionTimeout);
    }

    TimeValue getInitialRetryBackoffPeriod(TimeValue totalTimeout) {
        var initialRetryBackoffPeriod = (long) (totalTimeout.millis() * 0.05);
        if (initialRetryBackoffPeriod < MINIMUM_INITIAL_BACKOFF_PERIOD_MILLIS) {
            initialRetryBackoffPeriod = MINIMUM_INITIAL_BACKOFF_PERIOD_MILLIS;
        }
        return TimeValue.timeValueMillis(initialRetryBackoffPeriod);
    }

    private void executeRetryableAction(
        Task task,
        GetMeteringStatsAction.Request request,
        ActionListener<GetMeteringStatsAction.Response> listener,
        TimeValue timeout
    ) {
        ClusterState clusterState = clusterService.state();
        logger.trace("starting to process GetMeteringStatsAction request with cluster state version [{}]", clusterState.version());

        DiscoveryNode persistentTaskNode = findPersistentTaskNode(clusterState, persistentTaskName);
        DiscoveryNode localNode = clusterState.nodes().getLocalNode();
        if (persistentTaskNode == null) {
            listener.onFailure(new PersistentTaskNodeNotAssignedException(persistentTaskName));
        } else if (localNode.getId().equals(persistentTaskNode.getId())) {
            executor.execute(() -> {
                try {
                    final var shardsInfo = meteringIndexInfoService.getMeteringShardInfo();
                    final var concreteIndicesNames = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
                    final var dataStreamConcreteIndicesNames = DataStreamsActionUtil.resolveConcreteIndexNames(
                        indexNameExpressionResolver,
                        clusterState,
                        request.indices(),
                        request.indicesOptions()
                    );

                    final var allConcreteIndicesNames = Stream.concat(
                        concreteIndicesNames != null ? Arrays.stream(concreteIndicesNames) : Stream.empty(),
                        dataStreamConcreteIndicesNames
                    ).toArray(String[]::new);

                    listener.onResponse(createResponse(shardsInfo, clusterState, allConcreteIndicesNames));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            logger.trace("forwarding request [{}] to PersistentTask node [{}]", actionName, persistentTaskNode);
            ActionListenerResponseHandler<GetMeteringStatsAction.Response> handler = new ActionListenerResponseHandler<>(
                listener,
                GetMeteringStatsAction.Response::new,
                executor
            ) {
                @Override
                public void handleResponse(GetMeteringStatsAction.Response response) {
                    try {
                        ClusterState updatedClusterState = clusterService.state();
                        DiscoveryNode postActionPersistentTaskNode = findPersistentTaskNode(updatedClusterState, persistentTaskName);
                        if (postActionPersistentTaskNode == null) {
                            listener.onFailure(new PersistentTaskNodeNotAssignedException(persistentTaskName));
                        } else if (persistentTaskNode.getId().equals(postActionPersistentTaskNode.getId()) == false) {
                            logger.trace(
                                "PersistentTask [{}] changed from node [{}] to node [{}]",
                                persistentTaskName,
                                persistentTaskNode,
                                postActionPersistentTaskNode
                            );
                            listener.onFailure(new NotPersistentTaskNodeException(persistentTaskNode.getId(), persistentTaskName));
                        } else {
                            listener.onResponse(response);
                        }
                    } catch (Exception ex) {
                        listener.onFailure(ex);
                    }
                }

                @Override
                public void handleException(final TransportException exception) {
                    logger.trace(
                        () -> Strings.format(
                            "failure when forwarding request [%s] to PersistentTask [%s] node [%s]",
                            actionName,
                            persistentTaskName,
                            persistentTaskNode
                        ),
                        exception
                    );
                    listener.onFailure(exception);
                }
            };

            transportService.sendChildRequest(
                persistentTaskNode,
                actionName,
                request,
                task,
                TransportRequestOptions.timeout(timeout),
                handler
            );
        }
    }

    @Override
    protected void doExecute(Task task, GetMeteringStatsAction.Request request, ActionListener<GetMeteringStatsAction.Response> listener) {
        try {
            var persistentTaskNodeTransportActionTimeout = getPersistentTaskNodeTransportActionTimeout();
            var retryableAction = new RetryableAction<>(
                logger,
                transportService.getThreadPool(),
                getInitialRetryBackoffPeriod(persistentTaskNodeTransportActionTimeout),
                persistentTaskNodeTransportActionTimeout,
                listener,
                executor
            ) {
                private static final Set<Class<? extends Exception>> retryableExceptions = Set.of(
                    PersistentTaskNodeNotAssignedException.class,
                    NotPersistentTaskNodeException.class,
                    ActionTransportException.class,
                    BindTransportException.class
                );

                @Override
                public void tryAction(ActionListener<GetMeteringStatsAction.Response> listener) {
                    executeRetryableAction(task, request, listener, persistentTaskNodeTransportActionTimeout);
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
                        return false;
                    }
                    return retryableExceptions.stream().anyMatch(clazz -> clazz.isAssignableFrom(e.getClass()));
                }
            };
            retryableAction.run();
        } catch (Exception e) {
            logger.trace(() -> Strings.format("Failed to route/execute PersistentTask node action %s", actionName), e);
            listener.onFailure(e);
        }
    }

    static GetMeteringStatsAction.Response createResponse(
        MeteringIndexInfoService.CollectedMeteringShardInfo shardsInfo,
        ClusterState clusterState,
        String[] indicesNames
    ) {
        final var metadata = clusterState.getMetadata();

        long workingTotalDocCount = 0L;
        long workingTotalSizeInBytes = 0L;
        Map<String, GetMeteringStatsAction.MeteringStats> indexToStatsModifiableMap = new HashMap<>();
        Map<String, String> indexToDatastreamModifiableMap = new HashMap<>();
        Map<String, GetMeteringStatsAction.MeteringStats> datastreamToStatsModifiableMap = new HashMap<>();

        var shardIds = StreamSupport.stream(clusterState.routingTable().allShards(indicesNames).spliterator(), false)
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());

        for (var shardId : shardIds) {
            var shardInfo = shardsInfo.getMeteringShardInfoMap(shardId);

            String indexName = shardId.getIndexName();
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexName);

            long currentCount = shardInfo.docCount();
            long currentSize = shardInfo.sizeInBytes();
            workingTotalDocCount += currentCount;
            workingTotalSizeInBytes += currentSize;

            final boolean inDatastream = indexAbstraction != null && indexAbstraction.getParentDataStream() != null;
            if (inDatastream) {
                String datastreamName = indexAbstraction.getParentDataStream().getName();
                indexToDatastreamModifiableMap.put(indexName, datastreamName);

                datastreamToStatsModifiableMap.compute(datastreamName, (name, existingStatsForDatastream) -> {
                    if (existingStatsForDatastream == null) {
                        return new GetMeteringStatsAction.MeteringStats(currentSize, currentCount);
                    }
                    return new GetMeteringStatsAction.MeteringStats(
                        currentSize + existingStatsForDatastream.sizeInBytes(),
                        currentCount + existingStatsForDatastream.docCount()
                    );
                });
            }
            indexToStatsModifiableMap.compute(indexName, (name, existingStatsForIndex) -> {
                if (existingStatsForIndex == null) {
                    return new GetMeteringStatsAction.MeteringStats(currentSize, currentCount);
                }
                return new GetMeteringStatsAction.MeteringStats(
                    currentSize + existingStatsForIndex.sizeInBytes(),
                    currentCount + existingStatsForIndex.docCount()
                );
            });
        }
        return new GetMeteringStatsAction.Response(
            workingTotalDocCount,
            workingTotalSizeInBytes,
            Collections.unmodifiableMap(indexToStatsModifiableMap),
            Collections.unmodifiableMap(indexToDatastreamModifiableMap),
            Collections.unmodifiableMap(datastreamToStatsModifiableMap)
        );
    }
}
