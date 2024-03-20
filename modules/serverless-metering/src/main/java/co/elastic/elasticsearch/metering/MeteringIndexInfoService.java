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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.action.CollectMeteringShardInfoAction;
import co.elastic.elasticsearch.metering.action.MeteringShardInfo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MeteringIndexInfoService {
    private static final Logger logger = LogManager.getLogger(MeteringIndexInfoService.class);

    private enum CollectedMeteringShardInfoFlag {
        PARTIAL,
        STALE
    }

    record CollectedMeteringShardInfo(
        Map<ShardId, MeteringShardInfo> meteringShardInfoMap,
        Set<CollectedMeteringShardInfoFlag> meteringShardInfoStatus
    ) {}

    private final AtomicReference<CollectedMeteringShardInfo> collectedShardInfo = new AtomicReference<>();

    /**
     * Updates the internal storage of metering shard info, by performing a scatter-gather operation towards all (search) nodes
     */
    public void updateMeteringShardInfo(Client client) {
        logger.debug("Calling IndexSizeService#updateMeteringShardInfo");
        client.execute(CollectMeteringShardInfoAction.INSTANCE, new CollectMeteringShardInfoAction.Request(), new ActionListener<>() {
            @Override
            public void onResponse(CollectMeteringShardInfoAction.Response response) {
                Set<CollectedMeteringShardInfoFlag> status = EnumSet.noneOf(CollectedMeteringShardInfoFlag.class);
                if (response.isComplete() == false) {
                    status.add(CollectedMeteringShardInfoFlag.PARTIAL);
                }

                // TODO[lor]: ES-7851: create a new MeteringShardInfo from diffs.
                collectedShardInfo.set(new CollectedMeteringShardInfo(response.getShardInfo(), status));
                logger.debug(
                    () -> Strings.format(
                        "collected new metering shard info for shards [%s]",
                        response.getShardInfo().keySet().stream().map(ShardId::toString).collect(Collectors.joining(","))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                var previousSizes = collectedShardInfo.get();
                var status = EnumSet.copyOf(previousSizes.meteringShardInfoStatus());
                status.add(CollectedMeteringShardInfoFlag.STALE);
                collectedShardInfo.set(new CollectedMeteringShardInfo(previousSizes.meteringShardInfoMap(), status));
                logger.error("failed to collect metering shard info", e);
            }
        });
    }
}
