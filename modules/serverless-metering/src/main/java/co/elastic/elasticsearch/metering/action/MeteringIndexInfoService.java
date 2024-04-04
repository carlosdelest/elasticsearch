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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.metering.MeteringIndexInfoTaskExecutor.MINIMUM_METERING_INFO_UPDATE_PERIOD;

public class MeteringIndexInfoService {
    private static final Logger logger = LogManager.getLogger(MeteringIndexInfoService.class);

    enum CollectedMeteringShardInfoFlag {
        PARTIAL,
        STALE
    }

    record CollectedMeteringShardInfo(
        Map<ShardId, MeteringShardInfo> meteringShardInfoMap,
        Set<CollectedMeteringShardInfoFlag> meteringShardInfoStatus
    ) {
        static final CollectedMeteringShardInfo INITIAL = new CollectedMeteringShardInfo(
            Map.of(),
            Set.of(CollectedMeteringShardInfoFlag.STALE)
        );

        public MeteringShardInfo getMeteringShardInfoMap(ShardId shardId) {
            var shardInfo = meteringShardInfoMap.get(shardId);
            return Objects.requireNonNullElse(shardInfo, MeteringShardInfo.EMPTY);
        }
    }

    private final AtomicReference<CollectedMeteringShardInfo> collectedShardInfo = new AtomicReference<>(
        CollectedMeteringShardInfo.INITIAL
    );
    private volatile TimeValue meteringShardInfoUpdatePeriod = MINIMUM_METERING_INFO_UPDATE_PERIOD;

    public TimeValue getMeteringShardInfoUpdatePeriod() {
        return meteringShardInfoUpdatePeriod;
    }

    public void setMeteringShardInfoUpdatePeriod(TimeValue meteringShardInfoUpdatePeriod) {
        this.meteringShardInfoUpdatePeriod = meteringShardInfoUpdatePeriod;
    }

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

                // Create a new MeteringShardInfo from diffs.
                collectedShardInfo.getAndUpdate(current -> mergeShardInfo(current.meteringShardInfoMap(), response.getShardInfo(), status));
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

    static CollectedMeteringShardInfo mergeShardInfo(
        Map<ShardId, MeteringShardInfo> current,
        Map<ShardId, MeteringShardInfo> updated,
        Set<CollectedMeteringShardInfoFlag> status
    ) {
        var merged = Stream.concat(current.entrySet().stream(), updated.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TransportCollectMeteringShardInfoAction::mostRecent));
        return new CollectedMeteringShardInfo(merged, status);
    }

    CollectedMeteringShardInfo getMeteringShardInfo() {
        return collectedShardInfo.get();
    }
}
