/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.FloatArray;

import java.util.Arrays;
// end generated imports

/**
 * Block build of DenseVectorBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class DenseVectorBlockBuilder extends AbstractBlockBuilder implements DenseVectorBlock.Builder {

    private float[][] values;
    private int dimensions;


    DenseVectorBlockBuilder(int estimatedSize, BlockFactory blockFactory, int dimensions) {
        this(estimatedSize, blockFactory);
        this.dimensions = dimensions;
    }

    DenseVectorBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new float[initialSize][];
    }

    @Override
    public DenseVectorBlockBuilder appendDenseVector(float[] value) {
        checkDimensions(value);
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    private void checkDimensions(float[] value) {
        assert dimensions == 0 || value.length == dimensions : "expected ["
            + dimensions
            + "] but was ["
            + value.length
            + "]";
        dimensions = value.length;
    }

    @Override
    protected int elementSize() {
        return Float.BYTES * dimensions == 0 ? 3096 : dimensions;
    }

    @Override
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        values = Arrays.copyOf(values, newSize);
    }

    @Override
    public DenseVectorBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public DenseVectorBlockBuilder beginPositionEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DenseVectorBlockBuilder endPositionEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DenseVectorBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((DenseVectorBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     * <p>
     *     For single-position copies see {@link #copyFrom(DenseVectorBlock, int)}.
     * </p>
     */
    @Override
    public DenseVectorBlockBuilder copyFrom(DenseVectorBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        DenseVectorVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(DenseVectorBlock block, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            copyFrom(block, p);
        }
    }

    private void copyFromVector(DenseVectorVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendDenseVector(vector.getDenseVector(p));
        }
    }

    /**
     * Copy the values in {@code block} at {@code position}. If this position
     * has a single value, this'll copy a single value. If this positions has
     * many values, it'll copy all of them. If this is {@code null}, then it'll
     * copy the {@code null}.
     * <p>
     *     Note that there isn't a version of this method on {@link Block.Builder} that takes
     *     {@link Block}. That'd be quite slow, running position by position. And it's important
     *     to know if you are copying {@link BytesRef}s so you can have the scratch.
     * </p>
     */
    @Override
    public DenseVectorBlockBuilder copyFrom(DenseVectorBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendDenseVector(block.getDenseVector(i++));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendDenseVector(block.getDenseVector(i++));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public DenseVectorBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private DenseVectorBlock buildBigArraysBlock() {
        final DenseVectorBlock theBlock;
        int dimensions = valueCount == 0 || values[0] == null ? 0 : values[0].length;
        final FloatArray array = blockFactory.bigArrays().newFloatArray((long) valueCount * dimensions, false);

        int pos = 0;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] == null) {
                continue;
            }
            for (int j = 0; j < values[i].length; j++) {
                array.set(pos++, values[i][j]);
            }
        }
        if (isDense() && singleValued()) {
            theBlock = new DenseVectorBigArrayVector(array, positionCount, dimensions, blockFactory).asBlock();
        } else {
            theBlock = new DenseVectorBigArrayBlock(
                array,
                positionCount,
                dimensions,
                firstValueIndexes,
                nullsMask,
                mvOrdering,
                blockFactory
            );
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - array.ramBytesUsed());
        return theBlock;
    }

    @Override
    public DenseVectorBlock build() {
        try {
            finish();
            DenseVectorBlock theBlock;
            int dimensions = valueCount == 0 || values[0] == null ? 0 : values[0].length;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantDenseVectorBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newDenseVectorArrayVector(values, positionCount, dimensions, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newDenseVectorArrayBlock(
                    values, // stylecheck
                    dimensions,
                    positionCount,
                    firstValueIndexes,
                    nullsMask,
                    mvOrdering,
                    estimatedBytes
                );
            }
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }
}
