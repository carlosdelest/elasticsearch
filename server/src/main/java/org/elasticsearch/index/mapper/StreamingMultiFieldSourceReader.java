/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts multiple fields from {@code _source} bytes in a single streaming JSON parse.
 * Fills one block builder per field. This avoids the {@code Map<String, Object>} allocation
 * required by the standard {@link SourceValueFetcher} path, reducing GC pressure
 * proportional to document size × field count.
 *
 * <p>Loaders opt in by returning a non-null {@link BlockLoader.SourceFieldExtractor}
 * from {@link BlockLoader#sourceFieldExtractor()}.</p>
 */
public class StreamingMultiFieldSourceReader {

    /**
     * A single field to extract: the paths to look for in source, the builder to append into,
     * and the extractor that converts raw token text to the appropriate typed value.
     */
    public record Target(
        java.util.Set<String> paths,
        BlockLoader.Builder builder,
        BlockLoader.SourceFieldExtractor extractor
    ) {}

    /**
     * Reads all {@code targets} from {@code sourceBytes} in a single streaming parse,
     * filling each target's builder. Fields not found in source get a null appended.
     *
     * @param sourceBytes  raw {@code _source} bytes; may be {@code null} if source is unavailable
     * @param contentType  the content type (JSON, CBOR, SMILE, etc.)
     * @param targets      the fields to extract; must be non-empty
     */
    public static void read(BytesReference sourceBytes, XContentType contentType, List<Target> targets) throws IOException {
        if (sourceBytes == null || sourceBytes.length() == 0) {
            for (Target target : targets) {
                target.extractor().appendNull(target.builder());
            }
            return;
        }

        // Build path → (target, index) lookup map
        Map<String, Integer> pathToIdx = new HashMap<>();
        for (int i = 0; i < targets.size(); i++) {
            for (String path : targets.get(i).paths()) {
                pathToIdx.put(path, i);
            }
        }

        boolean[] found = new boolean[targets.size()];

        try (XContentParser parser = XContentHelper.createParserNotCompressed(
            XContentParserConfiguration.EMPTY, sourceBytes, contentType)) {

            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                parseObject(parser, new ArrayDeque<>(), pathToIdx, targets, found);
            }
        }

        // Append null for any target field not found in this document
        for (int i = 0; i < targets.size(); i++) {
            if (found[i] == false) {
                targets.get(i).extractor().appendNull(targets.get(i).builder());
            }
        }
    }

    /**
     * Recursively parses a JSON object, matching fields against {@code pathToIdx}.
     * The {@code path} deque tracks the current nesting (dot-joined = current path).
     */
    private static void parseObject(
        XContentParser parser,
        Deque<String> path,
        Map<String, Integer> pathToIdx,
        List<Target> targets,
        boolean[] found
    ) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                continue;
            }
            String fieldName = parser.currentName();
            path.addLast(fieldName);
            String dotPath = buildPath(path);

            XContentParser.Token valueToken = parser.nextToken();
            Integer targetIdx = pathToIdx.get(dotPath);

            if (targetIdx != null) {
                Target target = targets.get(targetIdx);
                readAndAppend(parser, valueToken, target, targetIdx, found);
            } else if (valueToken == XContentParser.Token.START_OBJECT) {
                // Recurse into nested object — might contain target sub-fields
                parseObject(parser, path, pathToIdx, targets, found);
            } else if (valueToken == XContentParser.Token.START_ARRAY) {
                // Non-target array: skip entirely to avoid unnecessary traversal
                parser.skipChildren();
            }
            // Scalar non-target values are automatically advanced past by the next nextToken() call

            path.removeLast();
        }
    }

    /**
     * Appends the value at the current parser position to the target's builder.
     */
    private static void readAndAppend(
        XContentParser parser,
        XContentParser.Token valueToken,
        Target target,
        int targetIdx,
        boolean[] found
    ) throws IOException {
        if (valueToken.isValue()) {
            if (valueToken == XContentParser.Token.VALUE_NULL) {
                target.extractor().appendNull(target.builder());
            } else {
                target.extractor().appendRawValue(parser.text(), target.builder());
            }
            found[targetIdx] = true;
        } else if (valueToken == XContentParser.Token.START_ARRAY) {
            readArray(parser, target);
            found[targetIdx] = true;
        } else if (valueToken == XContentParser.Token.START_OBJECT) {
            // Field is an object, not a scalar — skip it and append null
            parser.skipChildren();
            target.extractor().appendNull(target.builder());
            found[targetIdx] = true;
        }
        // Other tokens (e.g. END_OBJECT, END_ARRAY at top level) are unexpected here
    }

    /**
     * Reads all primitive values from a JSON array and appends them to the target builder.
     * Nested objects and arrays within the array are skipped.
     * Mirrors the single-value / multi-value logic in {@link BlockSourceReader}.
     */
    private static void readArray(XContentParser parser, Target target) throws IOException {
        List<String> values = null;
        String firstValue = null;

        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            XContentParser.Token token = parser.currentToken();
            if (token.isValue() && token != XContentParser.Token.VALUE_NULL) {
                String text = parser.text();
                if (firstValue == null) {
                    firstValue = text;
                } else {
                    if (values == null) {
                        values = new ArrayList<>();
                        values.add(firstValue);
                    }
                    values.add(text);
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }

        if (firstValue == null) {
            // Empty array or all-null array
            target.extractor().appendNull(target.builder());
        } else if (values == null) {
            // Single value — no multi-value entry needed
            target.extractor().appendRawValue(firstValue, target.builder());
        } else {
            // Multiple values
            target.builder().beginPositionEntry();
            for (String v : values) {
                target.extractor().appendRawValue(v, target.builder());
            }
            target.builder().endPositionEntry();
        }
    }

    /**
     * Builds a dot-separated path string from the current path stack.
     */
    private static String buildPath(Deque<String> path) {
        if (path.size() == 1) {
            return path.peekLast();
        }
        return String.join(".", path);
    }
}
