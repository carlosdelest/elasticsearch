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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.either;
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
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, 21L)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, 22L)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1, 23L))
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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(13L))
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
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1, null))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, new MeteringShardInfo(22L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(23L, 130L, 1, 1, null))
        );

        var client = mock(Client.class);
        doAnswer(new TestCollectMeteringShardInfoActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(22L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(23L))
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testShardInfoPartialUpdateWithNewUUID() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);
        var newShard3Id = new ShardId("index1", "index1UUID-2", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 2, null))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, new MeteringShardInfo(22L, 120L, 1, 1, null)),
            entry(newShard3Id, new MeteringShardInfo(23L, 130L, 1, 1, null))
        );

        var client = mock(Client.class);
        doAnswer(new TestCollectMeteringShardInfoActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(23L))
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testShardInfoPartialUpdateWithTwoUnknownUUIDs() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);

        var shard3Id1 = new ShardId("index1", "index1UUID-1", 3);
        var shard3Id2 = new ShardId("index1", "index1UUID-2", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard3Id1, new MeteringShardInfo(22L, 120L, 1, 1, null)),
            entry(shard3Id2, new MeteringShardInfo(23L, 130L, 1, 1, null))
        );

        var client = mock(Client.class);

        doAnswer(new TestCollectMeteringShardInfoActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(2));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(
                    is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id2)),
                    either(withSizeInBytes(22L)).or(withSizeInBytes(23L))
                )
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testShardInfoPartialUpdateWithOneOldOneNewUUIDs() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);

        var shard3Id = new ShardId("index1", "index1UUID", 3);
        var shard3Id1 = new ShardId("index1", "index1UUID-1", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(13L, 120L, 1, 1, null))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard3Id, new MeteringShardInfo(22L, 120L, 1, 2, null)),
            entry(shard3Id1, new MeteringShardInfo(23L, 130L, 1, 1, null))
        );

        var client = mock(Client.class);

        doAnswer(new TestCollectMeteringShardInfoActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(23L))
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
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1, null))
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, new MeteringShardInfo(22L, 120L, 1, 1, null)),
            entry(shard3Id, new MeteringShardInfo(23L, 130L, 1, 2, null))
        );

        var client = mock(Client.class);
        doAnswer(new TestCollectMeteringShardInfoActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectMeteringShardInfoAction.INSTANCE), any(), any());

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
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(13L))
            )
        );
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(
            secondRoundShardInfo.meteringShardInfoMap(),
            allOf(
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard1Id)), withSizeInBytes(11L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard2Id)), withSizeInBytes(12L)),
                hasEntry(is(MeteringIndexInfoService.ShardInfoKey.fromShardId(shard3Id)), withSizeInBytes(23L))
            )
        );
        assertThat(secondRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));
    }

    public void testPersistentTaskNodeChangeResetShardInfo() {
        var service = new MeteringIndexInfoService();
        var initialShardInfo = service.getMeteringShardInfo();

        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, new MeteringShardInfo(11L, 110L, 1, 1, null)),
            entry(shard2Id, new MeteringShardInfo(12L, 120L, 1, 2, null)),
            entry(shard3Id, new MeteringShardInfo(13L, 130L, 1, 1, null))
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

        var previousState = TestTransportActionUtils.createMockClusterStateWithPersistentTask(TestTransportActionUtils.LOCAL_NODE_ID);
        var currentState = TestTransportActionUtils.createMockClusterStateWithPersistentTask(
            previousState,
            TestTransportActionUtils.NON_LOCAL_NODE_ID
        );
        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));

        var afterNodeChangeShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo.meteringShardInfoMap(), anEmptyMap());

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(firstRoundShardInfo.meteringShardInfoMap(), aMapWithSize(3));

        assertThat(afterNodeChangeShardInfo, not(nullValue()));
        assertThat(afterNodeChangeShardInfo.meteringShardInfoMap(), anEmptyMap());
    }

    private Matcher<MeteringIndexInfoService.ShardInfoValue> withSizeInBytes(final long expected) {
        return new FeatureMatcher<>(equalTo(expected), "shard info with size", "size") {
            @Override
            protected Long featureValueOf(MeteringIndexInfoService.ShardInfoValue actual) {
                return actual.sizeInBytes();
            }
        };
    }

    private static class TestCollectMeteringShardInfoActionAnswer implements Answer<Object> {
        private final AtomicInteger requestNumber = new AtomicInteger();
        private final Map<ShardId, MeteringShardInfo> shardsInfo;
        private final Map<ShardId, MeteringShardInfo> shardsInfo2;

        TestCollectMeteringShardInfoActionAnswer(Map<ShardId, MeteringShardInfo> shardsInfo, Map<ShardId, MeteringShardInfo> shardsInfo2) {
            this.shardsInfo = shardsInfo;
            this.shardsInfo2 = shardsInfo2;
        }

        @Override
        public Object answer(InvocationOnMock answer) throws Throwable {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectMeteringShardInfoAction.Response>) answer.getArgument(2, ActionListener.class);
            var currentRequest = requestNumber.addAndGet(1);
            if (currentRequest == 1) {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo, List.of()));
            } else {
                listener.onResponse(new CollectMeteringShardInfoAction.Response(shardsInfo2, List.of()));
            }
            return null;
        }
    }
}
