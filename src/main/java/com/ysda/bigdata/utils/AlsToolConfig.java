package com.ysda.bigdata.utils;

import org.kohsuke.args4j.Option;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsToolConfig {
    @Option(name="-input", required=true, usage="Path to input file")
    private String inputFilePath;

    @Option(name="-output", required=true, usage="Path to output folder")
    private String outputDirectoryPath;

    @Option(name="-f", aliases={"--factorize"}, forbids={"-p"},
            usage="Run AlsTool in factorization mode")
    private boolean factorizationMode;

    @Option(name="-p", aliases={"--preprocess"}, forbids={"-f"},
            usage="Run AlsTool for data preprocessing")
    private boolean preparationMode;

    @Option(name="-s", aliases={"--separator"}, depends={"-p"},
            usage="Separator to split row-column-rating line")
    private String lineSeparator = ",";

    public enum ExecutionMode {
        FACTORIZATION,
        PREPROCESSING
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getOutputDirectoryPath() {
        return outputDirectoryPath;
    }

    public ExecutionMode getMode() {
        if (!preparationMode) {
            factorizationMode = true;
        }

        if (factorizationMode) {
            return ExecutionMode.FACTORIZATION;
        } else {
            return ExecutionMode.PREPROCESSING;
        }
    }

    public String getLineSeparator() {
        return lineSeparator;
    }
}

