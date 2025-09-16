/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
// end generated imports

/**
 * Vector implementation that stores an array of float[] values.
 * This class is generated. Edit {@code X-ArrayVector.java.st} instead.
 */
final class DenseVectorArrayVector extends AbstractVector implements DenseVectorVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DenseVectorArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(DenseVectorVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final float[][] values;
    private final int dimensions;

    DenseVectorArrayVector(float[][] values, int positionCount, int dimensions, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.dimensions = dimensions;
        this.values = values;
    }

    static DenseVectorArrayVector readArrayVector(int positions, int dimensions, StreamInput in, BlockFactory blockFactory)
        throws IOException {
        final long preAdjustedBytes = NUM_BYTES_ARRAY_HEADER + (long) positions * Float.BYTES * dimensions;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            float[][] values = new float[positions][dimensions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.readOptionalFloatArray();
            }
            final var block = new DenseVectorArrayVector(values, positions, dimensions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - preAdjustedBytes);
            success = true;
            return block;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-preAdjustedBytes);
            }
        }
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeOptionalFloatArray(values[i]);
        }
    }

    @Override
    public DenseVectorBlock asBlock() {
        return new DenseVectorVectorBlock(this);
    }

    @Override
    public float[] getDenseVector(int position) {
        return values[position];
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
    public DenseVectorVector filter(int... positions) {
        try (DenseVectorVector.Builder builder = blockFactory().newDenseVectorVectorBuilder(positions.length, dimensions)) {
            for (int pos : positions) {
                builder.appendDenseVector(values[pos]);
            }
            return builder.build();
        }
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
        throw new UnsupportedOperationException();
    }

    private long ramBytesEstimated(float[][] values) {
        return BASE_RAM_BYTES_USED + (NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * dimensions) * values.length;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
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
        String valuesString = IntStream.range(0, getPositionCount())
            .limit(10)
            .mapToObj(n -> String.valueOf(values[n]))
            .collect(Collectors.joining(", ", "[", getPositionCount() > 10 ? ", ...]" : "]"));
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + valuesString + ']';
    }

}
