/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.settings;

import java.util.Map;

public class EngineSettings {

    private Relevance relevance;

    public Relevance getRelevance() {
        return relevance;
    }

    public void setRelevance(Relevance relevance) {
        this.relevance = relevance;
    }

    public static EngineSettings parseFromSource(Map<String, Object> engineSettings) {
        // TODO Can we use a parser here instead of iterating over the field structure?
        @SuppressWarnings("unchecked")
        final Map<String, Object> relevanceSettings = (Map<String, Object>) engineSettings.get("relevance");
        Relevance relevance = Relevance.parseFromSource(relevanceSettings);

        EngineSettings result = new EngineSettings();
        result.setRelevance(relevance);

        return result;
    }

}
