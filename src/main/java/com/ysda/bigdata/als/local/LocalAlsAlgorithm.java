package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.*;

/**
 * Created by xakl on 13.04.2016.
 */
public class LocalAlsAlgorithm implements IAlsAlgorithm {

    private ISparseMatrix ratingMatrix;
    private ISparseMatrix transposedRatingMatrix;
    private IDenseMatrix rowFactorsMatrix;
    private IDenseMatrix colFactorsMatrix;
    private int numFactors;
    private double regCoefficient;

    @Override
    public void init(ISparseMatrix ratingMatrix, ISparseMatrix transposedRatingMatrix,
                     int numFactors, double regCoefficient) {
        this.ratingMatrix = ratingMatrix;
        this.transposedRatingMatrix = transposedRatingMatrix;
        this.numFactors = numFactors;
        this.regCoefficient = regCoefficient;
        this.rowFactorsMatrix = new DenseMatrix(ratingMatrix.getNumRows(), numFactors);
        this.colFactorsMatrix = new DenseMatrix(transposedRatingMatrix.getNumRows(), numFactors);
    }

    @Override
    public MatrixFactorizationResult doIterations(int numIterations) {
        MatrixFactorizationResult result = null;
        for (int i = 0; i < numIterations; i++) {
            result = doIteration();
        }
        return result;
    }

    private MatrixFactorizationResult doIteration() {
        for (SparseRow ratingsRow : ratingMatrix) {
            rowFactorsMatrix.setRow(ratingsRow.getRowIndex(), computeRowFromOptimization(ratingsRow, colFactorsMatrix));
        }
        for (SparseRow ratingsRow : transposedRatingMatrix) {
            colFactorsMatrix.setRow(ratingsRow.getRowIndex(), computeRowFromOptimization(ratingsRow, rowFactorsMatrix));
        }
        MatrixFactorizationResult result = new MatrixFactorizationResult();
        result.rowFactorsMatrix = rowFactorsMatrix;
        result.colFactorsMatrix = colFactorsMatrix;
        return result;
    }

    private double[] computeRowFromOptimization(SparseRow ratingsRow, IDenseMatrix factorMatrix) {
        IDenseMatrix rowFactorsSubmatrix = factorMatrix.getSubmatrix(ratingsRow.getColIndices());
        IDenseMatrix transposedRowFactorsSubmatrix = rowFactorsSubmatrix.transpose();
        double[] result = transposedRowFactorsSubmatrix
                .multiply(rowFactorsSubmatrix)
                .addDiag(regCoefficient)
                .inverse()
                .multiply(transposedRowFactorsSubmatrix)
                .multiply(ratingsRow.getValues());
        return result;
    }
}
