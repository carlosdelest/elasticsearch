/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class StreamingMultiFieldSourceReaderTests extends ESTestCase {

    // --- Simple builder for capturing appended values ---

    private static class CapturingBuilder implements BlockLoader.Builder {
        final List<Object> values = new ArrayList<>();
        List<Object> current = null; // non-null when inside beginPositionEntry

        @Override
        public BlockLoader.Block build() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder appendNull() {
            if (current != null) {
                current.add(null);
            } else {
                values.add(null);
            }
            return this;
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            current = new ArrayList<>();
            return this;
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            values.add(current);
            current = null;
            return this;
        }

        @Override
        public void close() {}

        public Object get(int i) {
            return values.get(i);
        }
    }

    /** Simple BytesRef-appending extractor for test use. */
    private static BlockLoader.SourceFieldExtractor bytesExtractor(String... paths) {
        return new BlockLoader.SourceFieldExtractor() {
            private final BytesRef scratch = new BytesRef();

            @Override
            public Set<String> sourcePaths() {
                return Set.of(paths);
            }

            @Override
            public void appendRawValue(String rawValue, BlockLoader.Builder builder) {
                int len = rawValue.length();
                if (scratch.bytes.length < len * 3) {
                    scratch.bytes = new byte[len * 3];
                }
                scratch.length = org.apache.lucene.util.UnicodeUtil.UTF16toUTF8(rawValue, 0, len, scratch.bytes);
                ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(scratch);
            }

            @Override
            public void appendNull(BlockLoader.Builder builder) {
                builder.appendNull();
            }
        };
    }

    private static class BytesRefCapturingBuilder extends CapturingBuilder implements BlockLoader.BytesRefBuilder {
        @Override
        public BlockLoader.BytesRefBuilder appendBytesRef(BytesRef value) {
            BytesRef copy = BytesRef.deepCopyOf(value);
            if (current != null) {
                current.add(copy);
            } else {
                values.add(copy);
            }
            return this;
        }
    }

    // --- Helpers ---

    private static BytesReference json(String json) {
        return new BytesArray(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static BytesRef br(String s) {
        return new BytesRef(s);
    }

    // --- Tests ---

    public void testFlatTwoFields() throws IOException {
        BytesRefCapturingBuilder b1 = new BytesRefCapturingBuilder();
        BytesRefCapturingBuilder b2 = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("field1"), b1, bytesExtractor("field1")),
            new StreamingMultiFieldSourceReader.Target(Set.of("field2"), b2, bytesExtractor("field2"))
        );

        StreamingMultiFieldSourceReader.read(
            json("{\"field1\": \"hello\", \"field2\": \"world\"}"),
            XContentType.JSON,
            targets
        );

        assertThat(b1.values, hasSize(1));
        assertThat(b1.values.get(0), equalTo(br("hello")));
        assertThat(b2.values, hasSize(1));
        assertThat(b2.values.get(0), equalTo(br("world")));
    }

    public void testMissingFieldAppendNull() throws IOException {
        BytesRefCapturingBuilder b1 = new BytesRefCapturingBuilder();
        BytesRefCapturingBuilder b2 = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("present"), b1, bytesExtractor("present")),
            new StreamingMultiFieldSourceReader.Target(Set.of("absent"), b2, bytesExtractor("absent"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"present\": \"value\"}"), XContentType.JSON, targets);

        assertThat(b1.values.get(0), equalTo(br("value")));
        assertThat(b2.values.get(0), nullValue());
    }

    public void testNestedDotPath() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("user.name"), b, bytesExtractor("user.name"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"user\": {\"name\": \"alice\", \"age\": 30}}"), XContentType.JSON, targets);

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), equalTo(br("alice")));
    }

    public void testArrayFieldSingleValue() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("tags"), b, bytesExtractor("tags"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"tags\": [\"only\"]}"), XContentType.JSON, targets);

        // Single value in array → no beginPositionEntry/endPositionEntry
        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), equalTo(br("only")));
    }

    @SuppressWarnings("unchecked")
    public void testArrayFieldMultipleValues() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("tags"), b, bytesExtractor("tags"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"tags\": [\"a\", \"b\", \"c\"]}"), XContentType.JSON, targets);

        // Multi-value array → wrapped in a list
        assertThat(b.values, hasSize(1));
        List<Object> multi = (List<Object>) b.values.get(0);
        assertThat(multi, hasSize(3));
        assertThat(multi.get(0), equalTo(br("a")));
        assertThat(multi.get(1), equalTo(br("b")));
        assertThat(multi.get(2), equalTo(br("c")));
    }

    public void testEmptyArrayAppendNull() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("tags"), b, bytesExtractor("tags"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"tags\": []}"), XContentType.JSON, targets);

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), nullValue());
    }

    public void testNullSourceAppendNull() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("field"), b, bytesExtractor("field"))
        );

        StreamingMultiFieldSourceReader.read(null, XContentType.JSON, targets);

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), nullValue());
    }

    public void testNullValueInSource() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("field"), b, bytesExtractor("field"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"field\": null}"), XContentType.JSON, targets);

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), nullValue());
    }

    public void testExtraFieldsIgnored() throws IOException {
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("wanted"), b, bytesExtractor("wanted"))
        );

        StreamingMultiFieldSourceReader.read(
            json("{\"ignored1\": \"x\", \"wanted\": \"yes\", \"ignored2\": {\"deep\": \"stuff\"}}"),
            XContentType.JSON,
            targets
        );

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), equalTo(br("yes")));
    }

    public void testMultiplePathsForOneTarget() throws IOException {
        // A field can have multiple source paths (e.g. multi-field aliases)
        BytesRefCapturingBuilder b = new BytesRefCapturingBuilder();

        List<StreamingMultiFieldSourceReader.Target> targets = List.of(
            new StreamingMultiFieldSourceReader.Target(Set.of("alias1", "alias2"), b, bytesExtractor("alias1", "alias2"))
        );

        StreamingMultiFieldSourceReader.read(json("{\"alias2\": \"found\"}"), XContentType.JSON, targets);

        assertThat(b.values, hasSize(1));
        assertThat(b.values.get(0), equalTo(br("found")));
    }
}
