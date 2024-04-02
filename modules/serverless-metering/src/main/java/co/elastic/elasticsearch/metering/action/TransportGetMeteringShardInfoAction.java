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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetMeteringShardInfoAction extends HandledTransportAction<
    GetMeteringShardInfoAction.Request,
    GetMeteringShardInfoAction.Response> {
    private final ShardReader shardReader;
    private final LocalNodeMeteringShardInfoCache localNodeMeteringShardInfoCache;

    @Inject
    public TransportGetMeteringShardInfoAction(
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        LocalNodeMeteringShardInfoCache localNodeMeteringShardInfoCache
    ) {
        super(
            GetMeteringShardInfoAction.NAME,
            false,
            transportService,
            actionFilters,
            GetMeteringShardInfoAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.shardReader = new ShardReader(indicesService);
        this.localNodeMeteringShardInfoCache = localNodeMeteringShardInfoCache;
    }

    @Override
    protected void doExecute(
        Task task,
        GetMeteringShardInfoAction.Request request,
        ActionListener<GetMeteringShardInfoAction.Response> listener
    ) {
        try {
            var shardSizes = shardReader.getMeteringShardInfoMap(localNodeMeteringShardInfoCache);
            listener.onResponse(new GetMeteringShardInfoAction.Response(shardSizes));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
