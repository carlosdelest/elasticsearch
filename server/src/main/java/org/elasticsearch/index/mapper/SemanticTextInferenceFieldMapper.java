/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class SemanticTextInferenceFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "semantic_text_inference";

    public static final String FIELD_NAME = "_semantic_text_inference";
    public static final String SPARSE_VECTOR_SUBFIELD_NAME = TaskType.SPARSE_EMBEDDING.toString();

    private static SemanticTextInferenceFieldMapper toType(FieldMapper in) {
        return (SemanticTextInferenceFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder() {
            super(FIELD_NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        private SemanticTextInferenceFieldType buildFieldType(MapperBuilderContext context) {
            return new SemanticTextInferenceFieldType(context.buildFullName(name), meta.getValue());
        }

        @Override
        public SemanticTextInferenceFieldMapper build(MapperBuilderContext context) {
            return new SemanticTextInferenceFieldMapper(name(), new SemanticTextInferenceFieldType(name(), meta.getValue()), copyTo, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(), notInMultiFields(CONTENT_TYPE));

    public static class SemanticTextInferenceFieldType extends SimpleMappedFieldType {

        private SparseVectorFieldType sparseVectorFieldType;

        public SemanticTextInferenceFieldType(String name, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
        }

        public SparseVectorFieldType getSparseVectorFieldType() {
            return this.sparseVectorFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return sparseVectorFieldType.termQuery(value, context);
        }
    }

    private SemanticTextInferenceFieldMapper(String simpleName, MappedFieldType mappedFieldType, CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        if (context.parser().currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[_semantic_text_inference] fields must be a json object, expected a START_OBJECT but got: "
                    + context.parser().currentToken()
            );
        }

        MapperBuilderContext mapperBuilderContext = new MapperBuilderContext(FIELD_NAME, false, false);

        // TODO Can we validate that semantic text fields have actual text values?
        for (XContentParser.Token token = context.parser().nextToken(); token != XContentParser.Token.END_OBJECT; token = context.parser()
            .nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("[semantic_text] fields expect an object with field names, found " + token);
            }

            String fieldName = context.parser().currentName();

            Mapper mapper = context.getMapper(fieldName);
            if (SemanticTextFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false) {
                throw new IllegalArgumentException(
                    "Found [" + fieldName + "] in inference values, but it is not registered as a semantic_text field type"
                );
            }

            context.parser().nextToken();
            SparseVectorFieldMapper sparseVectorFieldMapper = new SparseVectorFieldMapper.Builder(fieldName).build(mapperBuilderContext);
            sparseVectorFieldMapper.parse(context);
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextInferenceFieldType fieldType() {
        return (SemanticTextInferenceFieldType) super.fieldType();
    }
}
