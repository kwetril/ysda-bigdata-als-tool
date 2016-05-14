package com.ysda.bigdata.als;

/**
 * Created by xakl on 13.04.2016.
 */
public interface IAlsAlgorithm {
    void init(BaseAlsInitConfig config);
    MatrixFactorizationResult doIterations(int numIterations);
}
