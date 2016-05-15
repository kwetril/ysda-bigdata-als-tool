package com.ysda.bigdata.utils;

import org.kohsuke.args4j.Option;

import java.util.concurrent.ExecutorService;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsToolConfig {
    /** Config example
     *  -input data/preprocessed/matrix.dat -t-input data/preprocessed/transposed-matrix.dat -k 10 -r 0.01 -output data/factorized --factorize
     *  -input data/data.txt -output data/preprocessed -s "\t" --preprocess
     *  -input data/data.txt -output data/preprocessed --spark
     */


    @Option(name="-input", required=true, usage="Path to input file")
    private String inputFilePath;

    @Option(name="-t-input", depends={"-f"}, usage="Path to transposed rating matrix file")
    private String transposedInputFilePath;

    @Option(name="-k", depends={"-f"}, usage="Number of hidden factors to use during factorization")
    private int numFactors;

    @Option(name="-r", depends={"-f"}, usage="Regularization coefficient to use in ALS algorithm")
    private double regCoefficient;

    @Option(name="-output", required=true, usage="Path to output folder")
    private String outputDirectoryPath;

    @Option(name="-f", aliases={"--factorize"}, forbids={"-p"},
            usage="Run AlsTool to perform factorization operation")
    private boolean factorizationOperation;

    @Option(name="-p", aliases={"--preprocess"}, forbids={"-f"},
            usage="Run AlsTool to perform data preprocessing")
    private boolean preparationOperation;

    @Option(name="-L", aliases={"--local"}, forbids={"-S"},
            usage="Run AlsTool in local mode.")
    private boolean localMode;

    @Option(name="-S", aliases={"--spark"}, forbids={"-L"},
            usage="Run AlsTool in cluster mode using spark.")
    private boolean sparkMode;

    @Option(name="-m", aliases={"--master"}, depends={"-S"},
            usage="Define Spark master.")
    private String sparkMaster;

    @Option(name="-s", aliases={"--separator"}, depends={"-p"},
            usage="Separator to split row-column-rating line")
    private String lineSeparator = ",";

    @Option(name="-w", aliases={"--workers"}, forbids={"-S"},
            usage="Number of worker threads to use during computations (default value: 1).")
    private int numWorkers = 1;

    public int getNumFactors() {
        return numFactors;
    }

    public double getRegCoefficient() {
        return regCoefficient;
    }

    public enum OperationType {
        FACTORIZATION,
        PREPROCESSING
    }

    public enum ExecutionMode {
        LOCAL,
        SPARK
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getOutputDirectoryPath() {
        return outputDirectoryPath;
    }

    public OperationType getOperation() {
        if (!preparationOperation) {
            factorizationOperation = true;
        }

        if (factorizationOperation) {
            return OperationType.FACTORIZATION;
        } else {
            return OperationType.PREPROCESSING;
        }
    }

    public ExecutionMode getExecutionMode() {
        if (!sparkMode) {
            localMode = true;
        }

        if (localMode) {
            return ExecutionMode.LOCAL;
        } else {
            return ExecutionMode.SPARK;
        }
    }

    public String getTransposedInputFilePath() {
        return transposedInputFilePath;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public int getNumWorkers() {
        return numWorkers;
    }
}

