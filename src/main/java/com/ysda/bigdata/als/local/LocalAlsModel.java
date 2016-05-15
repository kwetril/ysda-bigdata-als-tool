package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.BaseAlsModel;

/**
 * Created by kwetril on 5/14/16.
 */
public class LocalAlsModel extends BaseAlsModel {
    private ISparseMatrix ratingMatrix;
    private ISparseMatrix transposedRatingMatrix;

    @Override
    public void init(BaseAlsInitConfig config) {
        super.init(config);
        LocalAslInitConfig localConfig = (LocalAslInitConfig) config;
        this.ratingMatrix = localConfig.ratingMatrix;
        this.transposedRatingMatrix = localConfig.transposedRatingMatrix;
    }

    @Override
    public void train(int numIterations) {
        for (int i = 0; i < numIterations; i++) {
            for (SparseRow ratingsRow : ratingMatrix) {
                rowFactorsMatrix.setRow(ratingsRow.getRowIndex(), computeRowFromOptimization(ratingsRow, colFactorsMatrix));
            }
            for (SparseRow ratingsRow : transposedRatingMatrix) {
                colFactorsMatrix.setRow(ratingsRow.getRowIndex(), computeRowFromOptimization(ratingsRow, rowFactorsMatrix));
            }
        }
    }

    private double[] computeRowFromOptimization(SparseRow ratingsRow, FactorMatrix factorMatrix) {
        DenseMatrix rowFactorsSubmatrix = factorMatrix.getSubmatrix(ratingsRow.getColIndices());
        DenseMatrix transposedRowFactorsSubmatrix = rowFactorsSubmatrix.transpose();
        double[] result = transposedRowFactorsSubmatrix
                .multiply(rowFactorsSubmatrix)
                .addDiag(regCoefficient)
                .inverse()
                .multiply(transposedRowFactorsSubmatrix)
                .multiply(ratingsRow.getValues());
        return result;
    }
}
