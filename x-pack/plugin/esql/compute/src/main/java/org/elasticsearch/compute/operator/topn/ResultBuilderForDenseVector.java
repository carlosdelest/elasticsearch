/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DenseVectorBlock;
import org.elasticsearch.compute.data.DoubleBlock;

/**
 * Builds the resulting {@link DoubleBlock} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
class ResultBuilderForDenseVector implements ResultBuilder {
    private final DenseVectorBlock.Builder builder;

    private final boolean inKey;

    private float[] scratch;

    /**
     * The value previously set by {@link #decodeKey}.
     */
    private float[] key;

    ResultBuilderForDenseVector(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
        this.builder = blockFactory.newDenseVectorBlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        assert inKey;
        key = TopNEncoder.DEFAULT_SORTABLE.decodeDenseVector(keys, key);
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.appendDenseVector(inKey ? key : readValueFromValues(values));
            default -> {
                throw new IllegalStateException("multi-valued dense_vector not supported");
            }
        }
    }

    private float[] readValueFromValues(BytesRef values) {
        scratch = TopNEncoder.DEFAULT_UNSORTABLE.decodeDenseVector(values, scratch);
        return scratch;
    }

    @Override
    public DenseVectorBlock build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderForDenseVector[inKey=" + inKey + "]";
    }

    @Override
    public void close() {
        builder.close();
    }
}
