package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.IAlsModel;
import com.ysda.bigdata.utils.FastScanner;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kwetril on 5/14/16.
 */
public class LocalAlsModel implements IAlsModel {
    private ISparseMatrix ratingMatrix;
    private ISparseMatrix transposedRatingMatrix;
    private FactorMatrix rowFactorsMatrix;
    private FactorMatrix colFactorsMatrix;
    private int numFactors;
    private double regCoefficient;

    @Override
    public void init(BaseAlsInitConfig config) {
        LocalAslInitConfig localConfig = (LocalAslInitConfig) config;
        this.ratingMatrix = localConfig.ratingMatrix;
        this.transposedRatingMatrix = localConfig.transposedRatingMatrix;
        this.numFactors = localConfig.numFactors;
        this.regCoefficient = localConfig.regCoefficient;
        this.rowFactorsMatrix = new FactorMatrix(numFactors);
        this.colFactorsMatrix = new FactorMatrix(numFactors);
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

    @Override
    public void save(String path) throws IOException {
        try(FileWriter fileWriter = new FileWriter(path)) {
            writeMatrix(fileWriter, rowFactorsMatrix);
            writeMatrix(fileWriter, colFactorsMatrix);
        }
    }

    private void writeMatrix(FileWriter fileWriter, FactorMatrix matrix) throws IOException {
        HashMap<String, double[]> data = matrix.getData();
        int rows = data.size();
        int cols = matrix.getNumFactors();
        fileWriter.write(String.format("%s %s\n", rows, cols));
        for (Map.Entry<String, double[]> row : data.entrySet()) {
            StringBuilder rowLine = new StringBuilder();
            rowLine.append(row.getKey());
            for (double value : row.getValue()) {
                rowLine.append('\t');
                rowLine.append(value);
            }
            rowLine.append('\n');
            fileWriter.write(rowLine.toString());
        }
    }

    private FactorMatrix readMatrix(FastScanner scanner) throws IOException {
        int rows = scanner.nextInt();
        int cols = scanner.nextInt();
        HashMap<String, double[]> data = new HashMap<>();
        int numFactors = cols;
        for (int i = 0; i < rows; i++) {
            String key = scanner.next();
            double[] values = new double[cols];
            for (int j = 0; j < cols; j++) {
                values[j] = scanner.nextDouble();
            }
            data.put(key, values);
        }
        return new FactorMatrix(data, numFactors);
    }

    @Override
    public void load(String path) throws IOException {
        FastScanner scanner = new FastScanner(path);
        rowFactorsMatrix = readMatrix(scanner);
        colFactorsMatrix = readMatrix(scanner);
        numFactors = rowFactorsMatrix.getNumFactors();
    }

    @Override
    public double predict(String user, String item) {
        double[] userValues = rowFactorsMatrix.getRow(user);
        double[] itemValues = colFactorsMatrix.getRow(item);
        double result = 0;
        for (int i = 0; i < userValues.length; i++) {
            result += userValues[i] * itemValues[i];
        }
        return result;
    }
}
