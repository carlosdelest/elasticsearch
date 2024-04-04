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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class MeteringIndexInfoServiceTests extends ESTestCase {
    public void testEmptyShardInfo() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectMeteringShardInfoAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(new CollectMeteringShardInfoAction.Response(Map.of(), List.of()));
            return null;
        }).when(client).execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

        service.updateMeteringShardInfo(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo.meteringShardInfoMap(), anEmptyMap());

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), anEmptyMap());
    }

    public void testInitialShardInfoUpdate() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1))
        );

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectMeteringShardInfoAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo, List.of()));
            return null;
        }).when(client).execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

        service.updateMeteringShardInfo(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo.meteringShardInfoMap(), anEmptyMap());

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(shard1Id), withSizeInBytes(11L)),
                hasEntry(is(shard2Id), withSizeInBytes(12L)),
                hasEntry(is(shard3Id), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testShardInfoPartialUpdate() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, new MeteringShardInfo(22L, 120L, 1, 2)),
            entry(shard3Id, new MeteringShardInfo(23L, 130L, 1, 1))
        );

        var client = mock(Client.class);
        var requestNumber = new AtomicInteger();
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectMeteringShardInfoAction.Response>) answer.getArgument(2, ActionListener.class);
            var currentRequest = requestNumber.addAndGet(1);
            if (currentRequest == 1) {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo, List.of()));
            } else {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo2, List.of()));
            }
            return null;
        }).when(client).execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

        service.updateMeteringShardInfo(client);
        var firstRoundShardInfo = service.getMeteringShardInfo();

        service.updateMeteringShardInfo(client);
        var secondRoundShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo.meteringShardInfoMap(), anEmptyMap());

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(shard1Id), withSizeInBytes(11L)),
                hasEntry(is(shard2Id), withSizeInBytes(12L)),
                hasEntry(is(shard3Id), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(shard1Id), withSizeInBytes(11L)),
                hasEntry(is(shard2Id), withSizeInBytes(22L)),
                hasEntry(is(shard3Id), withSizeInBytes(23L))
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testShardInfoUpdateWithLatest() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, new MeteringShardInfo(22L, 120L, 1, 1)),
            entry(shard3Id, new MeteringShardInfo(23L, 130L, 1, 2))
        );

        var client = mock(Client.class);
        var requestNumber = new AtomicInteger();
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectMeteringShardInfoAction.Response>) answer.getArgument(2, ActionListener.class);
            var currentRequest = requestNumber.addAndGet(1);
            if (currentRequest == 1) {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo, List.of()));
            } else {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo2, List.of()));
            }
            return null;
        }).when(client).execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

        service.updateMeteringShardInfo(client);
        var firstRoundShardInfo = service.getMeteringShardInfo();

        service.updateMeteringShardInfo(client);
        var secondRoundShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo.meteringShardInfoMap(), anEmptyMap());

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(shard1Id), withSizeInBytes(11L)),
                hasEntry(is(shard2Id), withSizeInBytes(12L)),
                hasEntry(is(shard3Id), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(shard1Id), withSizeInBytes(11L)),
                hasEntry(is(shard2Id), withSizeInBytes(12L)),
                hasEntry(is(shard3Id), withSizeInBytes(23L))
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    private Matcher<MeteringShardInfo> withSizeInBytes(final long expected) {
        return new FeatureMatcher<>(equalTo(expected), "shard info with size", "size") {
            @Override
            protected Long featureValueOf(MeteringShardInfo actual) {
                return actual.sizeInBytes();
            }
        };
    }
}
