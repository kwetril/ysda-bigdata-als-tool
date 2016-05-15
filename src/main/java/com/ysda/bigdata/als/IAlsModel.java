package com.ysda.bigdata.als;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by kwetril on 5/14/16.
 */
public interface IAlsModel {
    void init(BaseAlsInitConfig config);
    void train(int numIterations);
    void save(String path) throws IOException;
    void load(String path) throws IOException;
    double predict(String user, String item);
    void batchPredicition(String inputPath, String outputPath, String lineSeparator) throws IOException;
}
