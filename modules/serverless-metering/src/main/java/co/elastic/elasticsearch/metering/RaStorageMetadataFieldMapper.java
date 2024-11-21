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

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

/**
 * The field mapper for the {@code _rastorage} field. This doesn't allow any access,
 * as all access is done through lucene directly.
 */
public class RaStorageMetadataFieldMapper extends MetadataFieldMapper {

    public static final String FIELD_NAME = "_rastorage";

    private static final RaStorageMetadataFieldMapper INSTANCE = new RaStorageMetadataFieldMapper();

    static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    private static final class RaStorageMetaFieldType extends MappedFieldType {

        private RaStorageMetaFieldType() {
            super(FIELD_NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return FIELD_NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + FIELD_NAME + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on [" + FIELD_NAME + "]");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new UnsupportedOperationException("The [" + FIELD_NAME + "] field may not be queried directly");
        }
    }

    private RaStorageMetadataFieldMapper() {
        super(new RaStorageMetaFieldType());
    }

    @Override
    protected String contentType() {
        return FIELD_NAME;
    }
}
