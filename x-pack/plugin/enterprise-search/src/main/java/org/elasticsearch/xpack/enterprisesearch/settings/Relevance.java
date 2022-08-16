/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.settings;

import java.util.Map;
import java.util.Set;

public class Relevance {

    private SearchFields searchFields;

    private Boosts boosts;

    public SearchFields getSearchFields() {
        return searchFields;
    }

    public void setSearchFields(SearchFields searchFields) {
        this.searchFields = searchFields;
    }

    public Boosts getBoosts() {
        return boosts;
    }

    public void setBoosts(Boosts boosts) {
        this.boosts = boosts;
    }

    public static class SearchFields {

        private Map<String, SearchFieldSettings> searchFieldSettings;

        public Set<String> getFields() {
            return searchFieldSettings.keySet();
        }

        public SearchFieldSettings getFieldSettings(String fieldName) {
            return searchFieldSettings.get(fieldName);
        }

        public void setSearchFieldSettings(Map<String, SearchFieldSettings> searchFieldSettings) {
            this.searchFieldSettings = searchFieldSettings;
        }
    }

    public static class SearchFieldSettings {
        private float weight;

        public float getWeight() {
            return weight;
        }

        public void setWeight(float weight) {
            this.weight = weight;
        }
    }

    public static class Boosts {

    }
}
