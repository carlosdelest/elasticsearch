package org.elasticsearch.test.knn;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.core.PathUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone utility to quantize float vectors to int8 (byte) vectors.
 * Reads a binary file of float vectors and writes a binary file of quantized byte vectors.
 * Usage:
 *   java FloatToInt8 input-float-file output-byte-file dimension [similarity=DOT_PRODUCT] [confidence=1.0] [bits=7]
 * The input file is expected to be a sequence of float32 values (no headers).
 * The output file will be a sequence of bytes (no headers), one vector after another.
 */
public class FloatToInt8 {

    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                "Usage: java FloatToInt8 <input-float-file> <output-byte-file> <dimension> <num_vectors> "
                    + "[similarity=COSINE] [confidence=1.0] [bits=7]"
            );
            System.exit(1);
        }
        CLIArgs cliArgs = getCliArgs(args);

        quantizeFloatsToBytes(
            new CLIArgs(
                cliArgs.inputFile(),
                cliArgs.outputFile(),
                cliArgs.dimensions(),
                cliArgs.numVectors(),
                cliArgs.similarity(),
                cliArgs.bits(),
                cliArgs.confidence()
            )
        );
    }

    private static CLIArgs getCliArgs(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        int dimensions = Integer.parseInt(args[2]);
        int numVectors = Integer.parseInt(args[3]);
        VectorSimilarityFunction similarity = args.length > 4 ? VectorSimilarityFunction.valueOf(args[4]) : VectorSimilarityFunction.COSINE;
        byte bits = (byte) (args.length > 5 ? Integer.parseInt(args[5]) : 7);
        float confidence = args.length > 6 ? Float.parseFloat(args[6]) : calculateDefaultConfidenceInterval(numVectors);
        CLIArgs result = new CLIArgs(inputFile, outputFile, dimensions, numVectors, similarity, bits, confidence);
        return result;
    }

    private record CLIArgs(
        String inputFile,
        String outputFile,
        int dimensions,
        int numVectors,
        VectorSimilarityFunction similarity,
        byte bits,
        float confidence
    ) {}

    private static void quantizeFloatsToBytes(CLIArgs args) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        try (FileChannel in = FileChannel.open(PathUtils.get(args.inputFile()))) {
            KnnIndexer.VectorReader vectorReader = KnnIndexer.getVectorReader(
                args.inputFile(),
                in,
                VectorEncoding.FLOAT32,
                args.dimensions()
            );
            for (int i = 0; i < args.numVectors(); i++) {
                float[] v = new float[args.dimensions()];
                vectorReader.next(v);
                vectors.add(v);
            }
        }
        FloatVectorValues floatVectorValues = FloatVectorValues.fromFloats(vectors, args.dimensions());
        ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(floatVectorValues, args.confidence(), vectors.size(), args.bits());
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(args.outputFile()))) {
            byte[] quantized = new byte[args.dimensions()];
            for (float[] vector : vectors) {
                quantizer.quantize(vector, quantized, args.similarity());
                out.write(quantized);
            }
            out.flush();
        }
        System.out.println(
            "Quantized "
                + vectors.size()
                + " vectors of dimension "
                + args.dimensions()
                + " to "
                + args.bits()
                + " bits per component ("
                + args.outputFile()
                + ")"
        );

        // Check that we can read back the quantized vectors
        try (FileChannel in = FileChannel.open(PathUtils.get(args.outputFile()))) {
            KnnIndexer.VectorReader vectorReader = KnnIndexer.getVectorReader(
                args.outputFile(),
                in,
                VectorEncoding.BYTE,
                args.dimensions()
            );
            byte[] quantized = new byte[args.dimensions()];
            for (int i = 0; i < args.numVectors(); i++) {
                vectorReader.next(quantized);
            }
            System.out.println("Read " + args.numVectors() + " vectors from " + args.outputFile());
        }
    }

    private static float calculateDefaultConfidenceInterval(int vectorDimension) {
        return Math.max(MINIMUM_CONFIDENCE_INTERVAL, 1f - (1f / (vectorDimension + 1)));
    }
}
