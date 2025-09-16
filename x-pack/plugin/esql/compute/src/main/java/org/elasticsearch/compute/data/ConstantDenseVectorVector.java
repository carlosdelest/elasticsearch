/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
// end generated imports

/**
 * Vector implementation that stores a constant float[] value.
 * This class is generated. Edit {@code X-ConstantVector.java.st} instead.
 */
final class ConstantDenseVectorVector extends AbstractVector implements DenseVectorVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantDenseVectorVector.class);

    private final float[] value;

    ConstantDenseVectorVector(float[] value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public float[] getDenseVector(int position) {
        return value;
    }

    @Override
    public DenseVectorBlock asBlock() {
        return new DenseVectorVectorBlock(this);
    }

    @Override
    public DenseVectorVector filter(int... positions) {
        return blockFactory().newConstantDenseVectorVector(value, positions.length);
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
        try (DenseVectorBlock.Builder builder = blockFactory().newDenseVectorBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendDenseVector(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<DenseVectorBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new DenseVectorLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single(
                (DenseVectorBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount())
            );
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantDenseVectorBlockWith(value, positions.getPositionCount()));
        }
        return new DenseVectorLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DENSE_VECTOR;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public DenseVectorVector deepCopy(BlockFactory blockFactory) {
        return blockFactory.newConstantDenseVectorVector(value, getPositionCount());
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
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
    public int dimensions() {
        return value == null ? 0 : value.length;
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
