/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Vector that stores float[] values.
 * This class is generated. Edit {@code X-Vector.java.st} instead.
 */
public sealed interface DenseVectorVector extends Vector permits ConstantDenseVectorVector, ConstantNullVector, DenseVectorArrayVector,
    DenseVectorBigArrayVector {

    float[] getDenseVector(int position);

    @Override
    DenseVectorBlock asBlock();

    @Override
    DenseVectorVector filter(int... positions);

    @Override
    DenseVectorBlock keepMask(BooleanVector mask);

    /**
     * Make a deep copy of this {@link Vector} using the provided {@link BlockFactory},
     * likely copying all data.
     */
    @Override
    default DenseVectorVector deepCopy(BlockFactory blockFactory) {
        try (DenseVectorBlock.Builder builder = blockFactory.newDenseVectorBlockBuilder(getPositionCount(), dimensions())) {
            builder.copyFrom(asBlock(), 0, getPositionCount());
            builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
            return builder.build().asVector();
        }
    }

    @Override
    ReleasableIterator<? extends DenseVectorBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a DenseVectorVector, and both vectors are {@link #equals(DenseVectorVector, DenseVectorVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(DenseVectorVector)}. */
    @Override
    int hashCode();

    int dimensions();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the DenseVectorVector interface.
     */
    static boolean equals(DenseVectorVector vector1, DenseVectorVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getDenseVector(pos) != vector2.getDenseVector(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generates the hash code for the given vector. The hash code is computed from the vector's values.
     * This ensures that {@code vector1.equals(vector2)} implies that {@code vector1.hashCode()==vector2.hashCode()}
     * for any two vectors, {@code vector1} and {@code vector2}, as required by the general contract of
     * {@link Object#hashCode}.
     */
    static int hash(DenseVectorVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
        }
        return result;
    }

    /** Deserializes a Vector from the given stream input. */
    static DenseVectorVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final int dimensions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, dimensions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantDenseVectorVector(in.readFloatArray(), positions);
            case SERIALIZE_VECTOR_ARRAY -> DenseVectorArrayVector.readArrayVector(positions, dimensions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> DenseVectorBigArrayVector.readArrayVector(positions, dimensions, in, blockFactory);
            default -> {
                assert false : "invalid vector serialization type [" + serializationType + "]";
                throw new IllegalStateException("invalid vector serialization type [" + serializationType + "]");
            }
        };
    }

    /** Serializes this Vector to the given stream output. */
    default void writeTo(StreamOutput out) throws IOException {
        final int positions = getPositionCount();
        final var version = out.getTransportVersion();
        out.writeVInt(positions);
        out.writeVInt(dimensions());
        if (isConstant() && positions > 0) {
            out.writeByte(SERIALIZE_VECTOR_CONSTANT);
            out.writeFloatArray(getDenseVector(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DenseVectorArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DenseVectorBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static DenseVectorVector readValues(int positions, int dimensions, StreamInput in, BlockFactory blockFactory)
        throws IOException {
        try (var builder = blockFactory.newDenseVectorVectorFixedBuilder(dimensions, positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendDenseVector(i, in.readFloatArray());
            }
            return builder.build();
        }
    }

    private static void writeValues(DenseVectorVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeFloatArray(v.getDenseVector(i));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits FixedBuilder, DenseVectorVectorBuilder {
        /**
         * Appends a float[] to the current entry.
         */
        Builder appendDenseVector(float[] value);

        @Override
        DenseVectorVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits DenseVectorVectorFixedBuilder {
        /**
         * Appends a float[] to the current entry.
         */
        @Override
        FixedBuilder appendDenseVector(float[] value);

        FixedBuilder appendDenseVector(int index, float[] value);

    }
}
