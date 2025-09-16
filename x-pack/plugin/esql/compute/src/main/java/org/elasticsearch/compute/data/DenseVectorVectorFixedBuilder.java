/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link DenseVectorVector}s that never grows. Prefer this to
 * {@link DenseVectorVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Edit {@code X-VectorFixedBuilder.java.st} instead.
 */
public final class DenseVectorVectorFixedBuilder implements DenseVectorVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final float[][] values;
    private final long preAdjustedBytes;
    private int dimensions;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    DenseVectorVectorFixedBuilder(int size, int dimensions, BlockFactory blockFactory) {
        preAdjustedBytes = ramBytesUsed(size, dimensions);
        blockFactory.adjustBreaker(preAdjustedBytes);
        this.blockFactory = blockFactory;
        this.values = new float[size][dimensions];
    }

    @Override
    public DenseVectorVectorFixedBuilder appendDenseVector(float[] value) {
        assertDimensions(value);
        values[nextIndex++] = value;
        return this;
    }

    @Override
    public DenseVectorVectorFixedBuilder appendDenseVector(int idx, float[] value) {
        assertDimensions(value);
        values[idx] = value;
        return this;
    }

    private void assertDimensions(float[] value) {
        assert dimensions == 0 || value.length == dimensions : "expected ["
            + dimensions
            + "] but was ["
            + value.length
            + "]";
        dimensions = value.length;
    }

    private long ramBytesUsed(int size, int dimensions) {
        return DenseVectorArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Float.BYTES * dimensions
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(values.length, dimensions);
    }

    @Override
    public DenseVectorVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        closed = true;
        DenseVectorVector vector;
        if (values.length == 1) {
            vector = blockFactory.newConstantDenseVectorBlockWith(values[0], 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.newDenseVectorArrayVector(values, values.length, dimensions, preAdjustedBytes);
        }
        assert vector.ramBytesUsed() == preAdjustedBytes : "fixed Builders should estimate the exact ram bytes used";
        return vector;
    }

    @Override
    public void close() {
        if (closed == false) {
            // If nextIndex < 0 we've already built the vector
            closed = true;
            blockFactory.adjustBreaker(-preAdjustedBytes);
        }
    }

    public boolean isReleased() {
        return closed;
    }
}
