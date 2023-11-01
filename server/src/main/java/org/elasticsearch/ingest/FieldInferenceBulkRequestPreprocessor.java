/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.inference.InferenceAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.SemanticTextInferenceFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

public class FieldInferenceBulkRequestPreprocessor extends AbstractBulkRequestPreprocessor {

    public static final String SEMANTIC_TEXT_ORIGIN = "semantic_text";

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private final OriginSettingClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public FieldInferenceBulkRequestPreprocessor(
        Supplier<DocumentParsingObserver> documentParsingObserver,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(documentParsingObserver);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.client = new OriginSettingClient(client, SEMANTIC_TEXT_ORIGIN);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    protected void processIndexRequest(
        IndexRequest indexRequest,
        int slot,
        RefCountingRunnable refs,
        IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure
    ) {
        assert indexRequest.isFieldInferenceDone() == false;

        IngestDocument ingestDocument = newIngestDocument(indexRequest);
        List<String> fieldNames = ingestDocument.getSource()
            .entrySet()
            .stream()
            .filter(entry -> fieldNeedsInference(indexRequest, entry.getKey(), entry.getValue()))
            .map(Map.Entry::getKey)
            .toList();

        // Runs inference sequentially. This makes easier sync and removes the problem of having multiple
        // BulkItemResponses for a single bulk request in TransportBulkAction.unwrappingSingleItemBulkResponse
        runInferenceForFields(indexRequest, fieldNames, refs.acquire(), slot, ingestDocument, onFailure);

    }

    @Override
    public boolean needsProcessing(DocWriteRequest<?> docWriteRequest, IndexRequest indexRequest, Metadata metadata) {
        return (indexRequest.isFieldInferenceDone() == false)
            && indexRequest.sourceAsMap()
                .entrySet()
                .stream()
                .anyMatch(entry -> fieldNeedsInference(indexRequest, entry.getKey(), entry.getValue()));
    }

    @Override
    public boolean hasBeenProcessed(IndexRequest indexRequest) {
        return indexRequest.isFieldInferenceDone();
    }

    @Override
    public boolean shouldExecuteOnIngestNode() {
        return false;
    }

    private boolean fieldNeedsInference(IndexRequest indexRequest, String fieldName, Object fieldValue) {

        if (fieldValue instanceof String == false) {
            return false;
        }

        return getModelForField(indexRequest, fieldName) != null;
    }

    private String getModelForField(IndexRequest indexRequest, String fieldName) {
        // Check all indices related to request have the same model id
        String model = null;
        try {
            Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), indexRequest);
            for (Index index : indices) {
                String modelForIndex = indicesService.indexService(index).mapperService().mappingLookup().modelForField(fieldName);
                if ((modelForIndex == null)) {
                    return null;
                }
                if ((model != null) && modelForIndex.equals(model) == false) {
                    return null;
                }
                model = modelForIndex;
            }
        } catch (Exception e) {
            // There's a problem retrieving the index
            return null;
        }
        return model;
    }

    private void runInferenceForFields(
        IndexRequest indexRequest,
        List<String> fieldNames,
        Releasable ref,
        int position,
        final IngestDocument ingestDocument,
        BiConsumer<Integer, Exception> onFailure
    ) {
        // We finished processing
        if (fieldNames.isEmpty()) {
            updateIndexRequestSource(indexRequest, ingestDocument);
            indexRequest.isFieldInferenceDone(true);
            ref.close();
            return;
        }
        String fieldName = fieldNames.get(0);
        List<String> nextFieldNames = fieldNames.subList(1, fieldNames.size());
        final String fieldValue = ingestDocument.getFieldValue(fieldName, String.class);
        if (fieldValue == null) {
            // Run inference for next field
            runInferenceForFields(indexRequest, nextFieldNames, ref, position, ingestDocument, onFailure);
        }

        String modelForField = getModelForField(indexRequest, fieldName);
        assert modelForField != null : "Field " + fieldName + " has no model associated in mappings";

        // TODO Hardcoding task type, how to get that from model ID?
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            modelForField,
            fieldValue,
            Map.of()
        );

        client.execute(InferenceAction.INSTANCE, inferenceRequest, new ActionListener<>() {
            @Override
            public void onResponse(InferenceAction.Response response) {
                // Transform into another top-level subfield
                ingestDocument.setFieldValue(
                    SemanticTextInferenceFieldMapper.FIELD_NAME + "." + fieldName,
                    response.getResult().asMap(fieldName).get(fieldName)
                );

                // Run inference for next fields
                runInferenceForFields(indexRequest, nextFieldNames, ref, position, ingestDocument, onFailure);
            }

            @Override
            public void onFailure(Exception e) {
                // Wrap exception in an illegal argument exception, as there is a problem with the model or model config
                onFailure.accept(
                    position,
                    new IllegalArgumentException("Error performing inference for field [" + fieldName + "]: " + e.getMessage(), e)
                );
                ref.close();
            }
        });
    }
}
