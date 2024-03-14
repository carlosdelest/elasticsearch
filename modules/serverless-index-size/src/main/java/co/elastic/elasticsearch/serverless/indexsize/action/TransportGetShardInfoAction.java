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

package co.elastic.elasticsearch.serverless.indexsize.action;

import co.elastic.elasticsearch.serverless.indexsize.MeteringShardInfoService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetShardInfoAction extends HandledTransportAction<GetShardInfoAction.Request, GetShardInfoAction.Response> {
    private final ShardReader shardReader;
    private final MeteringShardInfoService meteringShardInfoService;

    @Inject
    public TransportGetShardInfoAction(
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        MeteringShardInfoService meteringShardInfoService
    ) {
        super(
            GetShardInfoAction.NAME,
            false,
            transportService,
            actionFilters,
            GetShardInfoAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.shardReader = new ShardReader(indicesService);
        this.meteringShardInfoService = meteringShardInfoService;
    }

    @Override
    protected void doExecute(Task task, GetShardInfoAction.Request request, ActionListener<GetShardInfoAction.Response> listener) {
        try {
            var shardSizes = shardReader.getShardSizes(meteringShardInfoService);
            listener.onResponse(new GetShardInfoAction.Response(shardSizes));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
