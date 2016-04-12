package com.ysda.bigdata;

import org.kohsuke.args4j.Option;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsToolConfig {
    @Option(name="-input", required=true, usage="Path to input file")
    private String inputFilePath;

    public String getInputFilePath() {
        return inputFilePath;
    }
}
