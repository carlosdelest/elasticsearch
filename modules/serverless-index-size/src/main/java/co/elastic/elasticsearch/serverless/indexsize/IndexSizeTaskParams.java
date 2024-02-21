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

package co.elastic.elasticsearch.serverless.indexsize;

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Encapsulates the parameters needed to start the IX task. Currently, no parameters are required.
 */
public class IndexSizeTaskParams implements PersistentTaskParams {

    public static final IndexSizeTaskParams INSTANCE = new IndexSizeTaskParams();

    public static final ObjectParser<IndexSizeTaskParams, Void> PARSER = new ObjectParser<>(IndexSizeTask.TASK_NAME, true, () -> INSTANCE);

    IndexSizeTaskParams() {}

    IndexSizeTaskParams(StreamInput ignored) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return IndexSizeTask.TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ServerlessTransportVersions.INDEX_SIZE_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) {}

    public static IndexSizeTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IndexSizeTaskParams;
    }
}
