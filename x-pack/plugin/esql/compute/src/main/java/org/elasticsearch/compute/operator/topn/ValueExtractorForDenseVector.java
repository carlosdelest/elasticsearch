/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DenseVectorBlock;
import org.elasticsearch.compute.data.DenseVectorVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Extracts non-sort-key values for top-n from their {@link DoubleBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class ValueExtractorForDenseVector implements ValueExtractor {
    static ValueExtractorForDenseVector extractorFor(TopNEncoder encoder, boolean inKey, DenseVectorBlock block) {
        DenseVectorVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForDenseVector.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForDenseVector.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;
    protected final int dimensions;

    ValueExtractorForDenseVector(TopNEncoder encoder, boolean inKey, int dimensions) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
        this.dimensions = dimensions;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, float[] value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeDenseVector(value, values);
    }

    static class ForVector extends ValueExtractorForDenseVector {
        private final DenseVectorVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, DenseVectorVector vector) {
            super(encoder, inKey, vector.dimensions());
            this.vector = vector;
        }

        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            writeCount(values, 1);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(vector.dimensions(), values);
            if (inKey) {
                // will read results from the key
                return;
            }
            actualWriteValue(values, vector.getDenseVector(position));
        }
    }

    static class ForBlock extends ValueExtractorForDenseVector {
        private final DenseVectorBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, DenseVectorBlock block) {
            super(encoder, inKey, block.dimensions());
            this.block = block;
        }

        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            int size = block.getValueCount(position);
            writeCount(values, size);
            if (size == 1 && inKey) {
                // Will read results from the key
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                actualWriteValue(values, block.getDenseVector(i));
            }
        }
    }
}
