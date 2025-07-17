package org.elasticsearch.test.knn;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.quantization.ScalarQuantizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone utility to quantize float vectors to int8 (byte) vectors.
 * Reads a binary file of float vectors and writes a binary file of quantized byte vectors.
 * Usage:
 *   java FloatToInt8 <input-float-file> <output-byte-file> <dimension> [similarity=DOT_PRODUCT] [confidence=1.0] [bits=7]
 * The input file is expected to be a sequence of float32 values (no headers).
 * The output file will be a sequence of bytes (no headers), one vector after another.
 */
public class FloatToInt8 {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: java FloatToInt8 <input-float-file> <output-byte-file> <dimension> [similarity=DOT_PRODUCT] [confidence=1.0] [bits=7]");
      System.exit(1);
    }
    String inputFile = args[0];
    String outputFile = args[1];
    int dim = Integer.parseInt(args[2]);
    VectorSimilarityFunction similarity = args.length > 3 ? VectorSimilarityFunction.valueOf(args[3]) : VectorSimilarityFunction.DOT_PRODUCT;
    float confidence = args.length > 4 ? Float.parseFloat(args[4]) : 1.0f;
    byte bits = (byte) (args.length > 5 ? Integer.parseInt(args[5]) : 7);

    List<float[]> vectors = new ArrayList<>();
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)))) {
      byte[] buf = new byte[dim * Float.BYTES];
      while (in.read(buf) == buf.length) {
        float[] v = new float[dim];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < dim; i++) {
          v[i] = bb.getFloat();
        }
        vectors.add(v);
      }
    }
    FloatVectorValues floatVectorValues = FloatVectorValues.fromFloats(vectors, dim);
    ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(floatVectorValues, confidence, vectors.size(), bits);
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
      byte[] quantized = new byte[dim];
      for (float[] v : vectors) {
        quantizer.quantize(v, quantized, similarity);
        out.write(quantized);
      }
    }
    System.out.println("Quantized " + vectors.size() + " vectors of dimension " + dim + " to " + bits + " bits per component (" + outputFile + ")");
  }
}
