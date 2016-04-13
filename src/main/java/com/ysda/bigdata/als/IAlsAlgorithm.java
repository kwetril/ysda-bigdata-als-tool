package com.ysda.bigdata.als;

/**
 * Created by xakl on 13.04.2016.
 */
public interface IAlsAlgorithm {
    void init(ISparseMatrix ratingMatrix, ISparseMatrix transposedRatingMatrix, int numFactors, double regCoefficient);
    MatrixFactorizationResult doIterations(int numIterations);
}
