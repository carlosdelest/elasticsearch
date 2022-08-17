/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.settings;

import java.util.HashMap;
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

    public static Relevance parseFromSource(Map<String, Object> relevanceSettings) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> searchFields = (Map<String, Object>) relevanceSettings.get("searchFields");
        SearchFields relevanceSearchFields = SearchFields.parseFromSource(searchFields);

        Relevance relevance = new Relevance();
        relevance.setSearchFields(relevanceSearchFields);
        return relevance;
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

        public static SearchFields parseFromSource(Map<String, Object> searchFields) {
            Map<String, SearchFieldSettings> searchFieldSettingsMap = new HashMap<>();
            for (Map.Entry<String, Object> searchFieldsEntry : searchFields.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> fieldSettingsValue = (Map<String, Object>) searchFieldsEntry.getValue();
                SearchFieldSettings fieldSettings = SearchFieldSettings.parseFromSource(fieldSettingsValue);
                searchFieldSettingsMap.put(searchFieldsEntry.getKey(), fieldSettings);
            }

            SearchFields relevanceSearchFields = new SearchFields();
            relevanceSearchFields.setSearchFieldSettings(searchFieldSettingsMap);
            return relevanceSearchFields;
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

        public static SearchFieldSettings parseFromSource(Map<String, Object> fieldSettingsValue) {
            SearchFieldSettings fieldSettings = new SearchFieldSettings();
            final Object weightValue = fieldSettingsValue.get("weight");
            if (weightValue != null) {
                Double weight;
                try {
                    weight = Double.valueOf(weightValue.toString());
                } catch (NumberFormatException e) {
                    weight = 1.0;
                }
                fieldSettings.setWeight(weight.floatValue());
            }
            return fieldSettings;
        }
    }

    public static class Boosts {

    }
}
