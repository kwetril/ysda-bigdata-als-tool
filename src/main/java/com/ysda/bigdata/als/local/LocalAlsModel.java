package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.BaseAlsModel;
import com.ysda.bigdata.utils.FastScanner;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

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

    @Override
    public void batchPredicition(String inputPath, String outputPath, String lineSeparator) throws IOException {
        FastScanner scanner = new FastScanner(inputPath);
        String line = scanner.nextLine();
        try (FileWriter output = new FileWriter(outputPath)) {
            while (line != null) {
                String[] parts = line.split(lineSeparator);
                double rating = predict(parts[0], parts[1]);
                output.write(String.format("%s\t%s\t%s\n", parts[0], parts[1], rating));
                line = scanner.nextLine();
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
