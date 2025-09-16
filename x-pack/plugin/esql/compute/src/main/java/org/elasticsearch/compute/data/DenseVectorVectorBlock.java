/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Block view of a {@link DenseVectorVector}. Cannot represent multi-values or nulls.
 * This class is generated. Edit {@code X-VectorBlock.java.st} instead.
 */
public final class DenseVectorVectorBlock extends AbstractVectorBlock implements DenseVectorBlock {

    private final DenseVectorVector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    DenseVectorVectorBlock(DenseVectorVector vector) {
        this.vector = vector;
    }

    @Override
    public DenseVectorVector asVector() {
        return vector;
    }

    @Override
    public float[] getDenseVector(int valueIndex) {
        return vector.getDenseVector(valueIndex);
    }

    @Override
    public int dimensions() {
        return vector.dimensions();
    }

    @Override
    public int getPositionCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public DenseVectorBlock filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public DenseVectorBlock keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public DenseVectorBlock deepCopy(BlockFactory blockFactory) {
        return vector.deepCopy(blockFactory).asBlock();
    }

    @Override
    public ReleasableIterator<? extends DenseVectorBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return vector.lookup(positions, targetBlockSize);
    }

    @Override
    public DenseVectorBlock expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DenseVectorBlock that) {
            return DenseVectorBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DenseVectorBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }

    @Override
    public void closeInternal() {
        assert (vector.isReleased() == false) : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }
}
