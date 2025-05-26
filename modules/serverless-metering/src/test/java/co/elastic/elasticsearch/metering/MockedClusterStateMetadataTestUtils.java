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

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedClusterStateMetadataTestUtils {

    public static Metadata mockedClusterStateMetadata(Metadata mock, IndexAbstraction... indices) {
        return mockedClusterStateMetadata(mock, () -> Arrays.asList(indices));
    }

    public static Metadata mockedClusterStateMetadata(Metadata mock, Supplier<Collection<IndexAbstraction>> indices) {
        final ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(projectMetadata.id()).thenReturn(Metadata.DEFAULT_PROJECT_ID);
        when(mock.projects()).thenReturn(Map.of(Metadata.DEFAULT_PROJECT_ID, projectMetadata));
        when(mock.getDefaultProject()).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenAnswer(
            a -> new TreeMap<>(indices.get().stream().collect(Collectors.toMap(IndexAbstraction::getName, identity())))
        );
        return mock;
    }

    public static IndexAbstraction mockedIndex(Index index, boolean isSystem, boolean isHidden) {
        return mockedIndex(index, isSystem, isHidden, null);
    }

    public static IndexAbstraction mockedIndex(Index index, boolean isSystem, boolean isHidden, String datastream) {
        IndexAbstraction indexAbstraction = mock();
        when(indexAbstraction.getName()).thenReturn(index.getName());
        when(indexAbstraction.isHidden()).thenReturn(isHidden);
        when(indexAbstraction.isSystem()).thenReturn(isSystem);
        if (datastream != null) {
            var dataStream = new DataStream(
                datastream,
                List.of(index),
                0,
                Map.of(),
                isHidden,
                false,
                isSystem,
                false,
                IndexMode.STANDARD,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null,
                List.of(),
                true,
                null
            );
            when(indexAbstraction.getParentDataStream()).thenReturn(dataStream);
        }
        return indexAbstraction;
    }
}
