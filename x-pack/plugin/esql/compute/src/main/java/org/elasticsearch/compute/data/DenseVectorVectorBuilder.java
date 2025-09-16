/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Builder for {@link DoubleVector}s that grows as needed.
 * This class is generated. Edit {@code X-VectorBuilder.java.st} instead.
 */
final class DenseVectorVectorBuilder extends AbstractVectorBuilder implements DenseVectorVector.Builder {

    private float[][] values;
    private final int dimensions;

    DenseVectorVectorBuilder(int estimatedSize, int dimensions, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2) * dimensions;
        adjustBreaker(initialSize);
        values = new float[Math.max(estimatedSize, 2)][dimensions];
        this.dimensions = dimensions;
    }

    @Override
    public DenseVectorVectorBuilder appendDenseVector(float[] value) {
        assert value.length == dimensions : "expected ["
            + dimensions
            + "] but was ["
            + value.length
            + "]";
        ensureCapacity();
        values[valueCount] = value;
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return Double.BYTES;
    }

    @Override
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        // Copies references, not the arrays themselves
        values = Arrays.copyOf(values, newSize);
    }

    @Override
    public DenseVectorVector build() {
        finish();
        DenseVectorVector vector;
        if (valueCount == 1) {
            vector = blockFactory.newConstantDenseVectorBlockWith(values[0], 1, estimatedBytes).asVector();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            vector = blockFactory.newDenseVectorArrayVector(values, valueCount, dimensions, estimatedBytes);
        }
        built();
        return vector;
    }
}
