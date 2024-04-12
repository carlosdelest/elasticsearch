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

package co.elastic.elasticsearch.metering.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class GetMeteringStatsAction {

    /*
     * Both these actions are protected at REST layer by operator privileges. The REST layer switches between them based on the
     * presence or not of secondary authentication in the REST request: the FOR_SECONDARY_USER variant requires secondary authentication
     * and will use the secondary user's privileges
     */
    public static final String FOR_SECONDARY_USER_NAME = "indices:monitor/get/metering/stats";
    public static final String FOR_PRIMARY_USER_NAME = "indices:admin/get/metering/stats";

    /**
     * The GetMeteringStats action to be used with secondary authentication; this action will use the secondary user's privileges
     */
    public static final ActionType<Response> FOR_SECONDARY_USER_INSTANCE = new ActionType<>(FOR_SECONDARY_USER_NAME);
    /**
     * The GetMeteringStats action to be used with primary (regular) authentication; this action will use the primary user's privileges
     */
    public static final ActionType<Response> FOR_PRIMARY_USER_INSTANCE = new ActionType<>(FOR_PRIMARY_USER_NAME);

    private GetMeteringStatsAction() {/* no instances */}

    public record MeteringStats(long sizeInBytes, long docCount) implements Writeable {

        MeteringStats(StreamInput streamInput) throws IOException {
            this(streamInput.readVLong(), streamInput.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(sizeInBytes);
            out.writeVLong(docCount);
        }
    }

    public static class Request extends ActionRequest implements IndicesRequest, IndicesRequest.Replaceable {

        private String[] indices;
        private static final IndicesOptions ALL_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().includeHidden(true))
            .build();

        public Request() {
            super();
            this.indices = null;
        }

        public Request(String[] indices) {
            super();
            this.indices = indices;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArrayNullable(indices);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return ALL_INDICES_OPTIONS;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        final long totalDocCount;
        final long totalSizeInBytes;
        final Map<String, GetMeteringStatsAction.MeteringStats> indexToStatsMap;
        final Map<String, String> indexToDatastreamMap;
        final Map<String, GetMeteringStatsAction.MeteringStats> datastreamToStatsMap;

        public Response(
            final long totalDocCount,
            long totalSizeInBytes,
            Map<String, GetMeteringStatsAction.MeteringStats> indexToStatsMap,
            Map<String, String> indexToDatastreamMap,
            Map<String, GetMeteringStatsAction.MeteringStats> datastreamToStatsMap
        ) {
            this.totalDocCount = totalDocCount;
            this.totalSizeInBytes = totalSizeInBytes;
            this.indexToStatsMap = indexToStatsMap;
            this.indexToDatastreamMap = indexToDatastreamMap;
            this.datastreamToStatsMap = datastreamToStatsMap;
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            totalDocCount = in.readLong();
            totalSizeInBytes = in.readLong();
            indexToStatsMap = in.readImmutableMap(StreamInput::readString, GetMeteringStatsAction.MeteringStats::new);
            indexToDatastreamMap = in.readImmutableMap(StreamInput::readString, StreamInput::readString);
            datastreamToStatsMap = in.readImmutableMap(StreamInput::readString, GetMeteringStatsAction.MeteringStats::new);
        }

        @Override
        public Iterator<ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(ChunkedToXContentHelper.singleChunk((builder, p) -> {
                builder.startObject();
                builder.startObject("_total");
                builder.field("num_docs", totalDocCount);
                builder.field("size_in_bytes", totalSizeInBytes);
                builder.endObject();
                builder.startArray("indices");
                return builder;
            }), Iterators.flatMap(indexToStatsMap.entrySet().iterator(), indexStats -> ChunkedToXContentHelper.singleChunk((builder, p) -> {
                builder.startObject();
                String indexName = indexStats.getKey();
                builder.field("name", indexName);
                String datastream = indexToDatastreamMap.get(indexName);
                if (datastream != null) {
                    builder.field("datastream", datastream);
                }
                builder.field("num_docs", indexStats.getValue().docCount());
                builder.field("size_in_bytes", indexStats.getValue().sizeInBytes());
                builder.endObject();
                return builder;
            })), ChunkedToXContentHelper.singleChunk((builder, p) -> {
                builder.endArray();
                builder.startArray("datastreams");
                return builder;
            }),
                Iterators.flatMap(
                    datastreamToStatsMap.entrySet().iterator(),
                    datastreamStats -> ChunkedToXContentHelper.singleChunk((builder, p) -> {
                        builder.startObject();
                        String datastream = datastreamStats.getKey();
                        builder.field("name", datastream);
                        builder.field("num_docs", datastreamStats.getValue().docCount());
                        builder.field("size_in_bytes", datastreamStats.getValue().sizeInBytes());
                        builder.endObject();
                        return builder;
                    })
                ),
                ChunkedToXContentHelper.singleChunk((builder, p) -> {
                    builder.endArray();
                    builder.endObject();
                    return builder;
                })
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalDocCount);
            out.writeLong(totalSizeInBytes);
            out.writeMap(indexToStatsMap, StreamOutput::writeString, (output, value) -> value.writeTo(output));
            out.writeMap(indexToDatastreamMap, StreamOutput::writeString, StreamOutput::writeString);
            out.writeMap(datastreamToStatsMap, StreamOutput::writeString, (output, value) -> value.writeTo(output));
        }

        public int hashCode() {
            return Objects.hash(indexToStatsMap, indexToDatastreamMap, datastreamToStatsMap);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj instanceof Response == false) {
                return false;
            }
            Response other = (Response) obj;
            return indexToStatsMap.equals(other.indexToStatsMap)
                && indexToDatastreamMap.equals(other.indexToDatastreamMap)
                && datastreamToStatsMap.equals(other.datastreamToStatsMap);
        }
    }
}
