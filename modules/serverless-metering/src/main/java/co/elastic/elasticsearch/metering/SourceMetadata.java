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

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;

import java.util.Map;

/**
 * Source metadata keys used in usage records for metering.
 */
public interface SourceMetadata {
    String INDEX = "index";
    String INDEX_UUID = "index_uuid";
    String DATASTREAM = "datastream";
    String SYSTEM_INDEX = "system_index";
    String HIDDEN_INDEX = "hidden_index";
    String PARTIAL = "partial";

    static Map<String, String> indexSourceMetadata(Index index, Map<String, IndexAbstraction> indicesLookup, SystemIndices systemIndices) {
        return indexSourceMetadata(index, indicesLookup, systemIndices, false);
    }

    static Map<String, String> indexSourceMetadata(
        Index index,
        Map<String, IndexAbstraction> indicesLookup,
        SystemIndices systemIndices,
        boolean isPartial
    ) {
        // note: this is intentionally not resolved via IndexAbstraction, see https://elasticco.atlassian.net/browse/ES-10384
        final var isSystemIndex = systemIndices.isSystemIndex(index.getName());
        final var indexAbstraction = indicesLookup.get(index.getName());
        final var datastream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;

        Map<String, String> sourceMetadata = Maps.newHashMapWithExpectedSize(5);
        sourceMetadata.put(SourceMetadata.INDEX, index.getName());
        sourceMetadata.put(SourceMetadata.INDEX_UUID, index.getUUID());
        sourceMetadata.put(SourceMetadata.SYSTEM_INDEX, Boolean.toString(isSystemIndex));
        if (indexAbstraction != null) {
            sourceMetadata.put(SourceMetadata.HIDDEN_INDEX, Boolean.toString(indexAbstraction.isHidden()));
        }
        if (datastream != null) {
            sourceMetadata.put(SourceMetadata.DATASTREAM, datastream.getName());
        }
        if (isPartial) {
            sourceMetadata.put(SourceMetadata.PARTIAL, Boolean.TRUE.toString());
        }
        return sourceMetadata;
    }
}
