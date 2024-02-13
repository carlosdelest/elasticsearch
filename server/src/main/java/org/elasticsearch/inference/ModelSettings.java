/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public record ModelSettings(String inferenceId, @Nullable Integer dimensions, @Nullable SimilarityMeasure similarity) {

    public static final String NAME = "model_settings";

    private static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    private static final ParseField DIMENSIONS_FIELD = new ParseField("dimensions");
    private static final ParseField SIMILARITY_FIELD = new ParseField("similarity");

    public ModelSettings(String inferenceId, Integer dimensions, SimilarityMeasure similarity) {
        Objects.requireNonNull(inferenceId, "inferenceId must not be null");
        this.inferenceId = inferenceId;
        this.dimensions = dimensions;
        this.similarity = similarity;
    }

    public ModelSettings(Model model) {
        this(model.getInferenceEntityId(), model.getServiceSettings().dimensions(), model.getServiceSettings().similarity());
    }

    public static ModelSettings parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<ModelSettings, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        String inferenceId = (String) args[0];
        Integer dimensions = (Integer) args[1];
        SimilarityMeasure similarity = args[2] == null ? null : SimilarityMeasure.fromString((String) args[2]);
        return new ModelSettings(inferenceId, dimensions, similarity);
    });
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), DIMENSIONS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SIMILARITY_FIELD);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> attrsMap = new HashMap<>();
        attrsMap.put(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        if (dimensions != null) {
            attrsMap.put(DIMENSIONS_FIELD.getPreferredName(), dimensions);
        }
        if (similarity != null) {
            attrsMap.put(SIMILARITY_FIELD.getPreferredName(), similarity);
        }
        return Map.of(NAME, attrsMap);
    }
}
