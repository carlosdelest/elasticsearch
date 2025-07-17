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

    private static final String QUANTIZATION_SUFFIX = "-quantized.vec";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                "Usage: java FloatToInt8 <docs-file> <num-docs> <query-file> <num-queries> <dimension> "
                    + "[similarity=COSINE] [confidence=1.0] [bits=7]"
            );
            System.exit(1);
        }
        CLIArgs cliArgs = getCliArgs(args);

        quantizeFloatsToBytes(cliArgs);
    }

    private static CLIArgs getCliArgs(String[] args) {
        String docsFile = args[0];
        int numDocs = Integer.parseInt(args[1]);
        String queryFile = args[2];
        int numQueries = Integer.parseInt(args[3]);
        int dimensions = Integer.parseInt(args[4]);
        VectorSimilarityFunction similarity = args.length > 5 ? VectorSimilarityFunction.valueOf(args[5]) : VectorSimilarityFunction.COSINE;
        byte bits = (byte) (args.length > 6 ? Integer.parseInt(args[6]) : 7);
        float confidence = args.length > 7 ? Float.parseFloat(args[7]) : calculateDefaultConfidenceInterval(numDocs);

        return new CLIArgs(docsFile, queryFile, dimensions, numDocs, numQueries, similarity, bits, confidence);
    }

    private record CLIArgs(
        String docsFile,
        String queryFile,
        int dimensions,
        int numDocs,
        int numQueries,
        VectorSimilarityFunction similarity,
        byte bits,
        float confidence
    ) {}

    private static void quantizeFloatsToBytes(CLIArgs args) throws IOException {
        ScalarQuantizer quantizer = quantizeFileFloatToBytes(args, args.docsFile(), args.numDocs(), null);
        quantizeFileFloatToBytes(args, args.queryFile(), args.numQueries(), quantizer);
    }

    private static ScalarQuantizer quantizeFileFloatToBytes(CLIArgs args, String inputFile, int numVectors, ScalarQuantizer quantizer)
        throws IOException {

        List<float[]> vectors = new ArrayList<>();
        String outputFile = inputFile + QUANTIZATION_SUFFIX;
        try (FileChannel in = FileChannel.open(PathUtils.get(inputFile))) {
            KnnIndexer.VectorReader vectorReader = KnnIndexer.getVectorReader(
                inputFile,
                in,
                VectorEncoding.FLOAT32,
                args.dimensions()
            );
            for (int i = 0; i < numVectors; i++) {
                float[] v = new float[args.dimensions()];
                vectorReader.next(v);
                vectors.add(v);
            }
        }
        FloatVectorValues floatVectorValues = FloatVectorValues.fromFloats(vectors, args.dimensions());
        if (quantizer == null) {
            quantizer = ScalarQuantizer.fromVectors(floatVectorValues, args.confidence(), vectors.size(), args.bits());
        }
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
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
                + inputFile
                + ")"
        );

        // Check that we can read back the quantized vectors
        try (FileChannel in = FileChannel.open(PathUtils.get(outputFile))) {
            KnnIndexer.VectorReader vectorReader = KnnIndexer.getVectorReader(
                args.queryFile(),
                in,
                VectorEncoding.BYTE,
                args.dimensions()
            );
            byte[] quantized = new byte[args.dimensions()];
            for (int i = 0; i < numVectors; i++) {
                vectorReader.next(quantized);
            }
            System.out.println("Checked " + numVectors + " vectors from " + outputFile);
        }

        return quantizer;
    }

    private static float calculateDefaultConfidenceInterval(int vectorDimension) {
        return Math.max(MINIMUM_CONFIDENCE_INTERVAL, 1f - (1f / (vectorDimension + 1)));
    }
}
