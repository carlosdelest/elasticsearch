/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.rankeval.RatedDocument.DocumentKey;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Definition of a particular query in the ranking evaluation request.<br>
 * This usually represents a single user search intent and consists of an id
 * (ideally human readable and referencing the search intent), the list of
 * indices to be queries and the {@link SearchSourceBuilder} that will be used
 * to create the search request for this search intent.<br>
 * Alternatively, a template id and template parameters can be provided instead.<br>
 * Finally, a list of rated documents for this query also needs to be provided.
 * <p>
 * The json structure in the rest request looks like this:
 * <pre>
 * {
 *    "id": "coffee_query",
 *    "request": {
 *        "query": {
 *            "match": { "beverage": "coffee" }
 *        }
 *    },
 *    "summary_fields": ["title"],
 *    "ratings": [
 *        {"_index": "my_index", "_id": "doc1", "rating": 0},
 *        {"_index": "my_index", "_id": "doc2","rating": 3},
 *        {"_index": "my_index", "_id": "doc3", "rating": 1}
 *    ]
 * }
 * </pre>
 */
public class RatedRequest implements Writeable, ToXContentObject {
    private final String id;
    private final List<String> summaryFields;
    private final List<RatedDocument> ratedDocs;
    /**
     * Search request to execute for this rated request. This can be null in
     * case the query is supplied as a template with corresponding parameters
     */
    @Nullable
    private final SearchSourceBuilder evaluationRequest;
    /**
     * Map of parameters to use for filling a query template, can be used
     * instead of providing testRequest.
     */
    private final Map<String, Object> params;
    @Nullable
    private String templateId;
    @Nullable
    private final RatingsProvider ratingsProvider;
    @Nullable
    private final SearchResponse searchResponse;

    /**
     * Create a rated request with template ids and parameters.
     *
     * @param id         a unique name for this rated request
     * @param ratedDocs  a list of document ratings
     * @param templateId a templare id
     * @param params     template parameters
     */
    public RatedRequest(String id, List<RatedDocument> ratedDocs, String templateId, Map<String, Object> params) {
        this(id, ratedDocs, null, templateId, params, null, null);
    }

    public RatedRequest(String id, String templateId, Map<String, Object> params, RatingsProvider ratingsProvider) {
        this(id, List.of(), null, templateId, params, ratingsProvider, null);
    }

    /**
     * Create a rated request using a {@link SearchSourceBuilder} to define the
     * evaluated query.
     *
     * @param id a unique name for this rated request
     * @param ratedDocs a list of document ratings
     * @param evaluatedQuery the query that is evaluated
     */
    public RatedRequest(String id, List<RatedDocument> ratedDocs, SearchSourceBuilder evaluatedQuery) {
        this(id, ratedDocs, evaluatedQuery, null, new HashMap<>(), null, null);
    }

    public RatedRequest(
        String id,
        SearchSourceBuilder evaluatedQuery,
        RatingsProvider ratingsProvider
    ) {
        this(id, List.of(), evaluatedQuery, null, null, ratingsProvider, null);
    }

    /**
     * Create a rated request using direct search results instead of a query or template.
     *
     * @param id a unique name for this rated request
     * @param ratingsProvider the ratings provider to use for this request
     * @param searchResponse the search results to use directly
     */
    public RatedRequest(String id, RatingsProvider ratingsProvider, SearchResponse searchResponse) {
        this(id, null, null, null, null, ratingsProvider, searchResponse);
    }

    private RatedRequest(
        String id,
        List<RatedDocument> ratedDocs,
        SearchSourceBuilder evaluatedQuery,
        String templateId, Map<String, Object> params,
        RatingsProvider ratingsProvider,
        SearchResponse searchResponse
    ) {
        if (params != null && (params.size() > 0 && evaluatedQuery != null)) {
            throw new IllegalArgumentException(
                "Ambiguous rated request: Set both, verbatim test request and test request " + "template parameters."
            );
        }
        if (templateId != null && evaluatedQuery != null) {
            throw new IllegalArgumentException(
                "Ambiguous rated request: Set both, verbatim test request and test request " + "template parameters."
            );
        }
        if ((params == null || params.size() < 1) && evaluatedQuery == null && searchResponse == null) {
            throw new IllegalArgumentException("Need to set at least test request, test request template parameters, or search results.");
        }
        if ((params != null && params.size() > 0) && templateId == null) {
            throw new IllegalArgumentException("If template parameters are supplied need to set id of template to apply " + "them to too.");
        }
        if (searchResponse == null) {
            validateEvaluatedQuery(evaluatedQuery);
        }
        // check that not two documents with same _index/id are specified
        Set<DocumentKey> docKeys = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (docKeys.add(doc.getKey()) == false) {
                String docKeyToString = doc.getKey().toString().replace("\n", "").replace("  ", " ");
                throw new IllegalArgumentException(
                    "Found duplicate rated document key [" + docKeyToString + "] in evaluation request [" + id + "]"
                );
            }
        }
        this.id = id;
        this.evaluationRequest = evaluatedQuery;
        this.ratedDocs = new ArrayList<>(ratedDocs);
        if (params != null) {
            this.params = new HashMap<>(params);
        } else {
            this.params = Collections.emptyMap();
        }
        this.templateId = templateId;
        this.summaryFields = new ArrayList<>();
        this.ratingsProvider = ratingsProvider;
        this.searchResponse = searchResponse;
    }

    static void validateEvaluatedQuery(SearchSourceBuilder evaluationRequest) {
        // ensure that testRequest, if set, does not contain aggregation, suggest or highlighting section
        if (evaluationRequest != null) {
            if (evaluationRequest.suggest() != null) {
                throw new IllegalArgumentException("Query in rated requests should not contain a suggest section.");
            }
            if (evaluationRequest.aggregations() != null) {
                throw new IllegalArgumentException("Query in rated requests should not contain aggregations.");
            }
            if (evaluationRequest.highlighter() != null) {
                throw new IllegalArgumentException("Query in rated requests should not contain a highlighter section.");
            }
            if (evaluationRequest.explain() != null && evaluationRequest.explain()) {
                throw new IllegalArgumentException("Query in rated requests should not use explain.");
            }
            if (evaluationRequest.profile()) {
                throw new IllegalArgumentException("Query in rated requests should not use profile.");
            }
        }
    }

    RatedRequest(StreamInput in) throws IOException {
        this.id = in.readString();
        evaluationRequest = in.readOptionalWriteable(SearchSourceBuilder::new);

        int intentSize = in.readInt();
        ratedDocs = new ArrayList<>(intentSize);
        for (int i = 0; i < intentSize; i++) {
            ratedDocs.add(new RatedDocument(in));
        }
        this.params = in.readGenericMap();
        int summaryFieldsSize = in.readInt();
        summaryFields = new ArrayList<>(summaryFieldsSize);
        for (int i = 0; i < summaryFieldsSize; i++) {
            this.summaryFields.add(in.readString());
        }
        this.templateId = in.readOptionalString();
        this.ratingsProvider = in.readOptionalWriteable(RatingsProvider::new);
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalWriteable(evaluationRequest);

        out.writeInt(ratedDocs.size());
        for (RatedDocument ratedDoc : ratedDocs) {
            ratedDoc.writeTo(out);
        }
        out.writeGenericMap(params);
        out.writeInt(summaryFields.size());
        for (String fieldName : summaryFields) {
            out.writeString(fieldName);
        }
        out.writeOptionalString(this.templateId);
        out.writeOptionalWriteable(this.ratingsProvider);
        out.writeOptionalWriteable(this.searchResponse);
    }

    public SearchSourceBuilder getEvaluationRequest() {
        return evaluationRequest;
    }

    /** return the user supplied request id */
    public String getId() {
        return id;
    }

    /** return the list of rated documents to evaluate. */
    public List<RatedDocument> getRatedDocs() {
        return Collections.unmodifiableList(ratedDocs);
    }

    /** return the parameters if this request uses a template, otherwise this will be empty. */
    public Map<String, Object> getParams() {
        return Collections.unmodifiableMap(this.params);
    }

    /** return the parameters if this request uses a template, otherwise this will be {@code null}. */
    public String getTemplateId() {
        return this.templateId;
    }

    /** returns a list of fields that should be included in the document summary for matched documents */
    public List<String> getSummaryFields() {
        return Collections.unmodifiableList(summaryFields);
    }

    public void addSummaryFields(List<String> summaryFieldsToAdd) {
        this.summaryFields.addAll(Objects.requireNonNull(summaryFieldsToAdd, "no summary fields supplied"));
    }

    /** return the ratings provider if present, otherwise null. */
    @Nullable
    public RatingsProvider getRatingsProvider() {
        return ratingsProvider;
    }

    @Nullable
    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField REQUEST_FIELD = new ParseField("request");
    private static final ParseField RATINGS_FIELD = new ParseField("ratings");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ParseField FIELDS_FIELD = new ParseField("summary_fields");
    private static final ParseField TEMPLATE_ID_FIELD = new ParseField("template_id");
    private static final ParseField RATINGS_PROVIDER_FIELD = new ParseField("ratings_provider");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RatedRequest, Predicate<NodeFeature>> PARSER = new ConstructingObjectParser<>(
        "request",
        a -> new RatedRequest(
            (String) a[0],
            (List<RatedDocument>) a[1],
            (SearchSourceBuilder) a[2],
            (String) a[4], (Map<String, Object>) a[3],
            (RatingsProvider) a[5],
            null
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> RatedDocument.fromXContent(p), RATINGS_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> new SearchSourceBuilder().parseXContent(p, false, c),
            REQUEST_FIELD
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), PARAMS_FIELD);
        PARSER.declareStringArray(RatedRequest::addSummaryFields, FIELDS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TEMPLATE_ID_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> RatingsProvider.fromXContent(p),
            RATINGS_PROVIDER_FIELD
        );
    }

    /**
     * parse from rest representation
     */
    public static RatedRequest fromXContent(XContentParser parser, Predicate<NodeFeature> clusterSupportsFeature) {
        return PARSER.apply(parser, clusterSupportsFeature);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params xContentParams) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), this.id);
        if (evaluationRequest != null) {
            builder.field(REQUEST_FIELD.getPreferredName(), this.evaluationRequest);
        }
        builder.startArray(RATINGS_FIELD.getPreferredName());
        for (RatedDocument doc : this.ratedDocs) {
            doc.toXContent(builder, xContentParams);
        }
        builder.endArray();
        if (this.templateId != null) {
            builder.field(TEMPLATE_ID_FIELD.getPreferredName(), this.templateId);
        }
        if (this.params.isEmpty() == false) {
            builder.startObject(PARAMS_FIELD.getPreferredName());
            for (Entry<String, Object> entry : this.params.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        if (this.summaryFields.isEmpty() == false) {
            builder.startArray(FIELDS_FIELD.getPreferredName());
            for (String field : this.summaryFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (this.ratingsProvider != null) {
            builder.field(RATINGS_PROVIDER_FIELD.getPreferredName(), this.ratingsProvider);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RatedRequest other = (RatedRequest) obj;

        return Objects.equals(id, other.id)
            && Objects.equals(evaluationRequest, other.evaluationRequest)
            && Objects.equals(summaryFields, other.summaryFields)
            && Objects.equals(ratedDocs, other.ratedDocs)
            && Objects.equals(params, other.params)
            && Objects.equals(templateId, other.templateId)
            && Objects.equals(ratingsProvider, other.ratingsProvider);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(id, evaluationRequest, summaryFields, ratedDocs, params, templateId, ratingsProvider);
    }

    /**
     * RatingsProvider: describes how to generate ratings for this request.
     */
    public static class RatingsProvider implements Writeable, ToXContentObject {
        private final String templateId;
        private final Map<String, Object> params;
        @Nullable
        private final SearchSourceBuilder searchSourceBuilder;

        public RatingsProvider(String templateId, Map<String, Object> params) {
            this.templateId = templateId;
            this.params = params == null ? Collections.emptyMap() : new HashMap<>(params);
            this.searchSourceBuilder = null;
        }

        public RatingsProvider(SearchSourceBuilder searchSourceBuilder) {
            this.templateId = null;
            this.params = Collections.emptyMap();
            this.searchSourceBuilder = searchSourceBuilder;
        }

        public RatingsProvider(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.templateId = in.readString();
                this.params = in.readGenericMap();
                this.searchSourceBuilder = null;
            } else {
                this.templateId = null;
                this.params = Collections.emptyMap();
                this.searchSourceBuilder = in.readOptionalWriteable(SearchSourceBuilder::new);
            }
        }

        public String getTemplateId() {
            return templateId;
        }

        public Map<String, Object> getParams() {
            return Collections.unmodifiableMap(params);
        }

        @Nullable
        public SearchSourceBuilder getSearchSourceBuilder() {
            return searchSourceBuilder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            boolean isTemplate = templateId != null;
            out.writeBoolean(isTemplate);
            if (isTemplate) {
                out.writeString(templateId);
                out.writeGenericMap(params);
            } else {
                out.writeOptionalWriteable(searchSourceBuilder);
            }
        }

        public static RatingsProvider fromXContent(XContentParser parser) throws IOException {
            String templateId = null;
            Map<String, Object> params = null;
            SearchSourceBuilder searchSourceBuilder = null;
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            XContentParser.Token token = parser.currentToken();
            if (token != Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT for ratings_provider");
            }
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if ("template_id".equals(fieldName)) {
                        templateId = parser.text();
                    } else if ("params".equals(fieldName)) {
                        params = parser.map();
                    } else if ("request".equals(fieldName)) {
                        searchSourceBuilder = new SearchSourceBuilder().parseXContent(parser, false, null);
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            if (templateId != null) {
                return new RatingsProvider(templateId, params);
            } else if (searchSourceBuilder != null) {
                return new RatingsProvider(searchSourceBuilder);
            } else {
                throw new IllegalArgumentException("ratings_provider must specify either template_id or request");
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (templateId != null) {
                builder.field("template_id", templateId);
                builder.field("params", this.params);
            } else if (searchSourceBuilder != null) {
                builder.field("request", searchSourceBuilder);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RatingsProvider that = (RatingsProvider) o;
            return Objects.equals(templateId, that.templateId)
                && Objects.equals(params, that.params)
                && Objects.equals(searchSourceBuilder, that.searchSourceBuilder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(templateId, params, searchSourceBuilder);
        }
    }
}
