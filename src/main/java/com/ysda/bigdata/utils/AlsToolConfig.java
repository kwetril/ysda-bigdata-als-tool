package com.ysda.bigdata.utils;

import org.kohsuke.args4j.Option;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsToolConfig {
    /** Config example
     *  -input data/preprocessed/matrix.dat -t-input data/preprocessed/transposed-matrix.dat -k 10 -l 0.01 -output data/factorized --factorize
     *  -input data/data.txt -output data/preprocessed -s "\t" --preprocess
     *  -input data/data.txt -output data/spark --spark -k 10 -l 0.01 -s "\t" -m "local[4]"
     *  -input data/data.txt -output data/spark -model data/spark --spark --ratings -s "\t" -m "local[4]"
     */

    @Option(name="-input", required=true, usage="Path to input file")
    private String inputFilePath;

    @Option(name="-t-input", depends={"-f"}, usage="Path to transposed rating matrix file")
    private String transposedInputFilePath;

    @Option(name="-k", forbids={"-p"}, usage="Number of hidden factors to use during factorization")
    private int numFactors;

    @Option(name="-l", aliases={"--lambda"}, forbids={"-p"}, usage="Regularization coefficient to use in ALS algorithm")
    private double regCoefficient;

    @Option(name="-output", required=true, usage="Path to output folder")
    private String outputDirectoryPath;

    @Option(name="-model", usage="Path to file with saved model.")
    private String modelPath;

    @Option(name="-f", aliases={"--factorize"}, forbids={"-p", "-r"},
            usage="Run AlsTool to perform factorization operation")
    private boolean factorizationOperation;

    @Option(name="-p", aliases={"--preprocess"}, forbids={"-f", "-r"},
            usage="Run AlsTool to perform data preprocessing")
    private boolean preparationOperation;

    @Option(name="-r", aliases={"--ratings"}, forbids={"-f", "-p"},
            usage="Run AlsTool to calculate ratings for given users and items.")
    private boolean ratingCalculationOperation;

    @Option(name="-L", aliases={"--local"}, forbids={"-S"},
            usage="Run AlsTool in local mode.")
    private boolean localMode;

    @Option(name="-S", aliases={"--spark"}, forbids={"-L"},
            usage="Run AlsTool in cluster mode using spark.")
    private boolean sparkMode;

    @Option(name="-m", aliases={"--master"}, depends={"-S"},
            usage="Define Spark master.")
    private String sparkMaster = "local";

    @Option(name="-s", aliases={"--separator"},
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
        PREPROCESSING,
        RATING_CALCULATION
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
        if (!preparationOperation && !ratingCalculationOperation) {
            factorizationOperation = true;
        }

        if (factorizationOperation) {
            return OperationType.FACTORIZATION;
        } else if (preparationOperation) {
            return OperationType.PREPROCESSING;
        } else {
            return OperationType.RATING_CALCULATION;
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
    
    public String getModelPath() {
        return modelPath;
    }
}

