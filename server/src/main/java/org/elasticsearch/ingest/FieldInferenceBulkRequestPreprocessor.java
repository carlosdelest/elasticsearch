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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.SemanticTextFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

        refs.acquire();
        // Inference responses can update the fields concurrently
        final Map<String, Object> sourceMap = new ConcurrentHashMap<>(indexRequest.sourceAsMap());
        try (var inferenceRefs = new RefCountingRunnable(() -> onInferenceComplete(refs, indexRequest, sourceMap))) {
            sourceMap.entrySet()
                .stream()
                .filter(entry -> fieldNeedsInference(indexRequest, entry.getKey(), entry.getValue()))
                .forEach(entry -> {
                    runInferenceForField(indexRequest, entry.getKey(), inferenceRefs, slot, sourceMap, onFailure);
                });
        }
    }

    private void onInferenceComplete(RefCountingRunnable refs, IndexRequest indexRequest, Map<String, Object> sourceMap) {
        updateIndexRequestSource(indexRequest, newIngestDocument(indexRequest, sourceMap));
        refs.close();
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
        IndexService indexService = indicesService.indexService(
            indexNameExpressionResolver.concreteSingleIndex(clusterService.state(), indexRequest)
        );
        return indexService.mapperService().mappingLookup().modelForField(fieldName);
    }

    private void runInferenceForField(
        IndexRequest indexRequest,
        String fieldName,
        RefCountingRunnable refs,
        int position,
        final Map<String, Object> sourceAsMap,
        BiConsumer<Integer, Exception> onFailure
    ) {
        final String fieldValue = (String) sourceAsMap.get(fieldName);
        if (fieldValue == null) {
            return;
        }

        refs.acquire();
        String modelForField = getModelForField(indexRequest, fieldName);
        assert modelForField != null : "Field " + fieldName + " has no model associated in mappings";

        // TODO Hardcoding task type, how to get that from model ID?
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            modelForField,
            fieldValue,
            Map.of()
        );

        final long startTimeInNanos = System.nanoTime();
        ingestMetric.preIngest();
        client.execute(InferenceAction.INSTANCE, inferenceRequest, ActionListener.runAfter(new ActionListener<InferenceAction.Response>() {
            @Override
            public void onResponse(InferenceAction.Response response) {
                // Transform into two subfields, one with the actual text and other with the inference
                Map<String, Object> newFieldValue = new HashMap<>();
                newFieldValue.put(SemanticTextFieldMapper.TEXT_SUBFIELD_NAME, fieldValue);
                newFieldValue.put(
                    SemanticTextFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME,
                    response.getResult().asMap(fieldName).get(fieldName)
                );
                sourceAsMap.put(fieldName, newFieldValue);
            }

            @Override
            public void onFailure(Exception e) {
                // Wrap exception in an illegal argument exception, as there is a problem with the model or model config
                onFailure.accept(
                    position,
                    new IllegalArgumentException("Error performing inference for field [" + fieldName + "]: " + e.getMessage(), e)
                );
                ingestMetric.ingestFailed();
            }
        }, () -> {
            // regardless of success or failure, we always stop the ingest "stopwatch" and release the ref to indicate
            // that we're finished with this document
            indexRequest.isFieldInferenceDone(true);
            final long ingestTimeInNanos = System.nanoTime() - startTimeInNanos;
            ingestMetric.postIngest(ingestTimeInNanos);
            refs.close();
        }));
    }
}
