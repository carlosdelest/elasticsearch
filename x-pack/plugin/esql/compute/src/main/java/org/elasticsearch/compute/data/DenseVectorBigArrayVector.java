/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link FloatArray}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Edit {@code X-BigArrayVector.java.st} instead.
 */
public final class DenseVectorBigArrayVector extends AbstractVector implements DenseVectorVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final FloatArray values;
    private final int dimensions;

    public DenseVectorBigArrayVector(FloatArray values, int positionCount, int dimensions, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.dimensions = dimensions;
    }

    static DenseVectorBigArrayVector readArrayVector(int positions, int dimensions, StreamInput in, BlockFactory blockFactory)
        throws IOException {
        FloatArray values = blockFactory.bigArrays().newFloatArray(positions, false);
        boolean success = false;
        try {
            values.fillWith(in);
            DenseVectorBigArrayVector vector = new DenseVectorBigArrayVector(values, positions, dimensions, blockFactory);
            blockFactory.adjustBreaker(vector.ramBytesUsed() - RamUsageEstimator.sizeOf(values));
            success = true;
            return vector;
        } finally {
            if (success == false) {
                values.close();
            }
        }
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        values.writeTo(out);
    }

    @Override
    public DenseVectorBlock asBlock() {
        return new DenseVectorVectorBlock(this);
    }

    @Override
    public float[] getDenseVector(int position) {
        float[] result = new float[dimensions];
        for (int d = 0; d < dimensions; d++) {
            result[d] = values.get((long) position * dimensions + d);
        }
        return result;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DENSE_VECTOR;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public DenseVectorVector filter(int... positions) {
        var blockFactory = blockFactory();
        final FloatArray filtered = blockFactory.bigArrays().newFloatArray((long) positions.length * dimensions);
        for (int i = 0; i < positions.length; i++) {
            for (int d = 0; d < dimensions; d++) {
                filtered.set((long) i * dimensions + d, values.get((long) positions[i] * dimensions + d));
            }
        }
        return new DenseVectorBigArrayVector(filtered, positions.length, dimensions, blockFactory);
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public DenseVectorBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new DenseVectorVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new DenseVectorVectorBlock(this);
            }
            return (DenseVectorBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (DenseVectorBlock.Builder builder = blockFactory().newDenseVectorBlockBuilder(getPositionCount(), dimensions())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendDenseVector(getDenseVector(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<DenseVectorBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new DenseVectorLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link DenseVectorArray} is adjusted outside
        // of this class.
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DenseVectorVector that) {
            return DenseVectorVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DenseVectorVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
